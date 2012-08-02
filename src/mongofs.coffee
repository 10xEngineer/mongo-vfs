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
    path = path.slice 0, -1 if path.charAt(path.length - 1) is '/'
    path
  isFolder = (path) -> path.charAt(path.length - 1) is '/'
  createStatEntry = (doc) ->
    name: doc.filename
    mime: doc.contentType
    path: doc.metadata.path
    href: stripTrailingSlash(doc.metadata.path) + "/" + doc.filename
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
      [dir, base] = extractName path = stripTrailingSlash path
      files.find
        'metadata.path': path
      .toArray (err, docs) ->
        return cb err if err
        unless _.isEmpty docs
          cb null, 
            filename    : base
            length      : docs.length
            contentType : 'inode/directory'
            metadata:
              path: dir
        else
          cb()
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
    files.find('metadata.path': stripTrailingSlash path).each (err, doc) ->
      return stream.emit 'error' if err
      return stream.emit 'end' unless doc
      stream.emit 'data', createStatEntry doc
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
              cb err, {}
        gs.close rename     
      readable.removeListener 'data', onData
      readable.removeListener 'end', onEnd
      _.forEach buffer, (event) ->
        readable.emit.apply readable, event
  
  copy: (path, options, cb) ->
    meta = {}
    @readfile options.from, {}, (err, readMeta) =>
      return cb err if err
      @mkfile path, readMeta, (err, writeMeta) ->
        cb err, meta
        
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
    renameFolder = (next) -> files.findAndModify 
      'metadata.path': options.from
    , []
    , $set: 
      'metadata.path': path
    , {}
    , next # end of renameFolder
    
    async.parallel [renameFile, renameFolder], cb # end of rename
  
  stat: (path, options, cb) ->
    searchForDir = -> exist.folder path, (err, dir) ->
      return cb err if err
      unless dir
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
      # Remove chunks
      chunks.remove files_id: doc._id, (err) ->
        return cb err if err
        # Remove file itself
        files.remove _id: doc._id, (err) ->
          return cb err if err
          cb()
    
module.exports = MongoFS