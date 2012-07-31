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
  files   = null
  chunks  = null
  db      = null
  extractName = (path) -> [ Path.dirname(path), Path.basename path ]
  
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
    files.find('metadata.path': path).each (err, doc) ->
      stream.emit 'data', doc.filename
    
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
      cursor = chunks.find 
        files_id: doc._id
      # This is another object in args saying 'give me only data'
      , ['data']
      cursor.each (err, chunk) ->
        return stream.emit 'error', err if err
        return stream.emit 'end' unless chunk
        stream.emit 'data', chunk.data.buffer
    cb null, {stream}
  mkfile: (path, options, cb) ->
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
      metadata:  
        path: dir
    gs.open (err) ->
      readable.on 'data', (chunk) ->
        gs.write chunk, (err) ->
      stream = gs.stream()
      stream.on 'end', (cb = ->) -> 
        gs.close cb
      readable.on 'end', ->
        cb null, 
          tmpPath: "#{dir}/#{temp}"
          stream: stream
      readable.removeListener 'data', onData
      readable.removeListener 'end', onEnd
      _.forEach buffer, (event) ->
        readable.emit.apply readable, event
        
  rename: (path, options, cb) ->
    from = extractName options.from
    to = extractName path
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
    
module.exports = MongoFS