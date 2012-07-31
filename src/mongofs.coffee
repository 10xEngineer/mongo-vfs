mongodb     = require 'mongodb'
Path        = require 'path'
_           = require 'underscore'
EventEmitter= require('events').EventEmitter
Stream      = require 'stream'
Db          = mongodb.Db
Server      = mongodb.Server
Connection  = mongodb.Connection
GridStore   = mongodb.GridStore

class MongoFS
  files = null
  chunks = null
  db = null
  extractName = (path) -> [ Path.dirname(path), Path.basename path ]
  constructor: (@options) ->
    server = new Server @options.host, @options.port, {}
    db = new Db @options.database, server    
    files = db.collection 'fs.files'
    chunks = db.collection 'fs.chunks'
    # gs = new GridStore db, 'root.coffee', 'w', 
    #   content_type: 'application/coffee'
    #   metadata:
    #     path: '/folder'
    #     bucketId: '412hab211-121bafhf'
    #     user: 'Jan Fabian'
    #   chunk_size: 1024*4
    # db.open (err) ->
    #   console.log err
    #   gs.open (err, gs) ->
    #     console.log err
    #     gs.write 'bar', (err, gs) ->
    #       gs.close (err) ->
    #         db.close()
  open: (cb) -> 
    db.open cb
  readdir: (path, cb) -> 
    files.find('metadata.path': path).toArray cb
  readfile: (path, options, cb) ->
    [dir, base] = extractName path
    query = 
      filename: base
      'metadata.path': dir
    files.findOne query
    , (err, doc) ->
      return cb err, {} if err
      return cb err, {} unless doc
      console.log doc
      stream = new Stream()
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
    gs = new GridStore db, base, 'w', 
      matadata: 
        path: dir
    gs.open (err) ->
      readable.on 'data', (chunk) ->
        gs.write chunk, (err) ->
      readable.on 'end', ->
        gs.close cb
      readable.removeListener 'data', onData
      readable.removeListener 'end', onEnd
      _.forEach buffer, (event) ->
        readable.emit.apply readable, event
    
module.exports = MongoFS