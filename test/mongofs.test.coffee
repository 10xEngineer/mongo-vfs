MongoFS     = require '../src/mongofs'
mongodb     = require 'mongodb'
Stream      = require 'stream'
Path        = require 'path'
Db          = mongodb.Db
Server      = mongodb.Server
Connection  = mongodb.Connection
GridStore   = mongodb.GridStore

describe 'mongo-vfs', ->
  credentials = 
    database: 'mongofs_test'
    host: 'localhost'
    port: Connection.DEFAULT_PORT
  
  mfs = new MongoFS credentials
  
  # Another access to test mongo database
  db      = null
  files   = null
  chunks  = null
  
  before (done) -> 
    # clear screen
    process.stdout.write '\u001B[2J\u001B[0;0f'
    mfs.open done
    
  beforeEach (done) ->
    server = new Server credentials.host, credentials.port, {}
    db = new Db credentials.database, server
    db.open (err, db) ->
      files = db.collection 'fs.files'
      chunks = db.collection 'fs.chunks'
      files.remove()
      chunks.remove()
      addFile done
    addFile = (next) ->
      gs = new GridStore db, 'bar', 'w', 
        metadata: 
          path: '/folder'
      gs.open (err) ->
        gs.write 'foo', (err) ->
          gs.close next
          
  describe 'readfile', ->        
    it 'should read a file', (done) ->
      mfs.readfile '/folder/bar', {}, (err, meta) ->
        done err if err
        data = []
        meta.stream.on 'data', (chunk) ->
          data.push chunk
        meta.stream.on 'end', ->
          data.join().should.eql 'foo'
          done()
          
  describe 'mkfile', ->
    it 'should create a new temp file and return stream to it', (done) ->
      stream = new Stream()
      mfs.mkfile '/bar', {stream}, (err, meta) ->
        done err if err
        meta.should.have.property 'tmpPath'
        meta.should.have.property 'stream'
        meta.stream.emit 'end', ->
          cursor = files.find 
            filename: Path.basename meta.tmpPath
            'metadata.path': Path.dirname meta.tmpPath
          cursor.toArray (err, docs) ->
            done err if err
            docs.should.not.be.empty
            done()
      stream.emit 'data', 'Hello'
      stream.emit 'end'
  
  describe 'rename', ->
    it 'should rename a file', (done) ->
      mfs.rename '/baz', {from:'/folder/bar'}, (err) ->
        done err if err
        # Now there should be /baz file
        cursor = files.find 
          filename: 'baz'
          'metadata.path': '/'
        cursor.toArray (err, docs) ->
          done err if err
          docs.should.not.be.empty
          # ... and /folder/bar should be gone
          cursor = files.find 
            filename: 'bar'
            'metadata.path': '/folder'
          cursor.toArray (err, docs) ->
            done err if err
            docs.should.be.empty
            done()  
            
    it 'should rename a directory', (done) ->
      mfs.rename '/baz', {from:'/folder'}, (err) ->
        done err if err
        # Now there should be /baz file
        cursor = files.find 
          'metadata.path': '/baz'
        cursor.toArray (err, docs) ->
          done err if err
          docs.should.not.be.empty
          # ... and /folder/bar should be gone
          cursor = files.find 
            filename: 'bar'
            'metadata.path': '/folder'
          cursor.toArray (err, docs) ->
            done err if err
            docs.should.be.empty
            done()  
  
  # describe 'readdir', ->
  #   it 'should list all files and folders in directory', (done) ->
  #     mfs.readdir

