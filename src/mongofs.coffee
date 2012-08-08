mongodb     = require 'mongodb'
Path        = require 'path'
_           = require 'underscore'
EventEmitter= require('events').EventEmitter
Stream      = require 'stream'
async       = require 'async'
Db          = mongodb.Db
Server      = mongodb.Server
Connection  = mongodb.Connection
GridStore   = mongodb.GridStore

class MongoFS
  # Private variables
  files   = null
  chunks  = null
  db      = null
  # Private functions
  extractName = (path) -> [ Path.dirname(path), Path.basename path ]
  stripTrailingSlash = (path) ->
    if (path.length isnt 1) and path.charAt(path.length - 1) is '/'
      path = path.slice 0, -1 
    path
  addTrailingSlash = (path) -> 
    path += '/' unless path.charAt(path.length - 1) is '/'
    path
  createStatEntry = (doc) ->
    href = addTrailingSlash(doc.metadata.path) + doc.filename
    name: doc.filename
    mime: doc.contentType
    path: stripTrailingSlash doc.metadata.path
    href: href
    size: doc.length
  exist = 
    file: (path, cb = ->) ->
      [dir, base] = extractName path
      files.findOne
        filename: base
        'metadata.path': dir
      , (err, doc) ->
        return cb err if err
        if doc
          cb null, doc
        else
          cb()
    folder: (path, cb = ->) ->
      [dir, base] = extractName stripTrailingSlash path
      iterateThroughFolder path, (err, docs) ->
        return cb err if err
        res = 
          filename    : base
          contentType : 'inode/directory'
          length      : docs.length
          metadata:
            path: dir
        cb null, res
  iterateThroughFolder = (path, cb = ->) ->
    [dir, base] = extractName path = stripTrailingSlash path
    subFiles = (next) ->
      # Don't count .empty file
      files.find
        filename: $ne: '.empty'
        'metadata.path': path
      .toArray next
    subFolders = (next) ->
      files.find
        filename: '.empty'
        'metadata.path': new RegExp "^#{addTrailingSlash path}\\w+$"
      .toArray (err, docs) ->
        return next err if err
        if _.isArray(docs) and not _.isEmpty docs
          docs = _.uniq docs, false, (doc) -> doc.metadata.path
          docs = _.map docs, (doc) ->
            [dir, base] = extractName doc.metadata.path
            doc.filename = base
            doc.metadata.path = dir
            doc.contentType = 'inode/directory'
            doc
        next null, docs
      # end of subFolders
    async.parallel [subFiles, subFolders], (err, results) ->
      return cb err if err
      cb null, Array.prototype.concat.apply [], results
    
  # end of private functions
  
  # Public API
  constructor: (@options) ->
    server = new Server @options.host, @options.port, {}
    db = new Db @options.database, server
  open: (cb) -> 
    db.open (err, db) -> 
      db      = db
      files   = db.collection 'fs.files'
      chunks  = db.collection 'fs.chunks'
      cb.apply null, arguments
  readdir: (path, options, cb) ->
    stream = new Stream()
    stream.readable = true
    cb null, {stream}
    iterateThroughFolder path, (err, docs) ->
      return stream.emit 'error', err if err
      iterator = (doc, next) ->
        createStatsAndEmit = ->
          stream.emit 'data', createStatEntry doc
          next()
        if doc.contentType is 'inode/directory'
          subFolderPath = addTrailingSlash(doc.metadata.path) + doc.filename
          exist.folder subFolderPath, (err, _doc) ->
            doc = _doc
            createStatsAndEmit()
        else
          createStatsAndEmit()
      async.forEach docs, iterator, (err) ->
        return stream.emit 'error', err if err
        stream.emit 'end'
  readfile: (path, options, cb) ->
    [dir, base] = extractName path
    stream = new Stream()
    query = 
      filename: base
      'metadata.path': dir
    files.findOne query
    , (err, doc) ->
      return cb err, {} if err
      return cb err, {} unless doc
      cb null, 
        stream: stream
        mime  : doc.contentType
      cursor = chunks.find 
        files_id: doc._id
      # This is another object in args saying 'give me only data'
      , ['data']
      cursor.each (err, chunk) ->
        return stream.emit 'error', err if err
        return stream.emit 'end' unless chunk
        stream.emit 'data', chunk.data.buffer
  mkfile: (path, options, cb) ->
    that = this
    [dir, base] = extractName path
    buffer = []
    meta = {}
    (readable = options.stream).on 'data', onData = (chunk) ->
      buffer.push ['data', chunk]
    readable.on 'end', onEnd = ->
      buffer.push ['end']
    temp = "#{base}."
    temp+= "#{Date.now().toString 36}-"
    temp+= "#{(Math.random() * 0x100000000).toString 36}" 
    gs = new GridStore db, temp, 'w', 
      content_type: options.mime 
      metadata:  
        path: dir
    gs.open (err) ->
      readable.on 'data', (chunk) ->
        gs.write chunk, (err) ->
      readable.on 'end', ->
        rename = -> exist.file path, (err, doc) ->
          # The file already exists or other error
          if doc
            # We return the temp file
            cb new Error 'File already exists'          
          else
            that.rename path, {from: "#{dir}/#{temp}"}, (err) ->
              cb err, meta
          # end of rename
        gs.close rename     
      readable.removeListener 'data', onData
      readable.removeListener 'end', onEnd
      _.forEach buffer, (event) ->
        readable.emit.apply readable, event
      # end of gs.open
    # end of mkfile
  mkdir: (path, options, cb) ->
    [dir, base] = extractName path = stripTrailingSlash path
    iterateThroughFolder dir, (err, docs) =>
      return cb err if err
      return cb new Error "Directory #{dir} doesnt exist" if _.isEmpty docs
      if _.any(docs, (doc) -> 
        (doc.contentType is 'inode/directory') and doc.filename is base)
        return cb new Error "Directory #{path} already exists"
      stream = new Stream()
      stream.readable = true
      @mkfile addTrailingSlash(path) + '.empty', {stream}, cb
      stream.emit 'end'
    # end of mkdir
  copy: (path, options, cb) ->
    meta = {}
    @readfile options.from, {}, (err, readMeta) =>
      return cb err if err
      @mkfile path, readMeta, (err, writeMeta) ->
        cb err, meta
    # end of copy
  rename: (path, options, cb) ->
    from = extractName (options.from = stripTrailingSlash options.from)
    to = extractName (path = stripTrailingSlash path)
    # Rename file
    renameFile = (next) -> files.findAndModify 
    # query
      filename: from[1]
      'metadata.path': from[0]
    # sort order
    , []
    # name it properly
    , $set: 
        filename: to[1]
        'metadata.path': to[0]
    , {}
    , next # end of renameFile
    
    # Rename folder
    renameFolder = (next) -> files.update 
      'metadata.path': options.from
    , $set: 
        'metadata.path': path
    , 
      multi : true
      safe  : true
    , next # end of renameFolder
    
    async.parallel [renameFile, renameFolder], cb # end of rename
  
  stat: (path, options, cb) ->
    searchForDir = -> exist.folder path, (err, dir) ->
      return cb err if err
      unless dir.length > 0
        cb new Error 'File does not exist'
      else
        cb null, createStatEntry dir
    searchForFile = -> exist.file path, (err, doc) ->
      return cb err if err
      unless doc
        searchForDir()
      else
        cb null, createStatEntry doc
        
    searchForFile()
  
  rmfile: (path, options, cb) ->
    [dir, base] = extractName path
    # Find the file
    files.findOne
      filename: base
      'metadata.path': dir
    , (err, doc) ->
      return cb err if err
      # Remove file itself
      files.remove _id: doc._id, (err) ->
        return cb err if err
        # Note: mongodb.remove() sometimes fire up callback, but docs
        # are still in database, therefore defer (aka setTimeout 0)
        # to postpone next action 343
        _.defer ->      
          # Remove chunks
          chunks.remove files_id: doc._id, (err) ->
            return cb err if err
            _.defer cb
        # end of files.remove callback
      # end of findOne callback
  
  rmdir: (path, options = {}, cb) ->
    path = stripTrailingSlash path
    options.recursive ?= false
    files.find
      'metadata.path': path
    .toArray (err, docs) ->
      empty = 
        (docs.length is 1) && 
        (docs[0].filename is '.empty') &&
        (docs[0].length is 0)
      if options.recursive or empty
        files.remove
          'metadata.path': new RegExp '^' + path
        , (err) -> _.defer cb, err
      else
        cb new Error 'Directory is not empty'
        
  # End of public API
  
module.exports = MongoFS