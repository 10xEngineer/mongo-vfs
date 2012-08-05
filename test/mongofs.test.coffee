MongoFS     = require '../src/mongofs'
mongodb     = require 'mongodb'
Stream      = require 'stream'
Path        = require 'path'
async       = require 'async'
assert      = require 'assert'
_           = require 'underscore'
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
      async.parallel [
        (next) -> 
          db.collection 'fs.files', (err, _files) ->
            files = _files
            files.remove (err) -> _.defer next, err
        (next) -> 
          db.collection 'fs.chunks', (err, _chunks) ->
            chunks = _chunks
            chunks.remove (err) -> _.defer next, err
        addDirectory '/folder'
        addFile '/folder', 'bar'
        addFile '/folder', 'bar2'
        addDirectory '/folder/folder2'
        addDirectory '/empty'
        addCoffeeFile
      ], done
    addFile = (path, filename, content = 'foo') ->
      (next) ->
        gs = new GridStore db, filename, 'w', 
          metadata: 
            path: path
        gs.open (err) ->
          gs.write content, (err) ->
            gs.close next
    addDirectory = (path) ->
      (next) -> 
        gs = new GridStore db, '.empty', 'w', 
          metadata: 
            path: path
        gs.open (err) ->
          gs.close next
    addCoffeeFile = (next) ->      
      gs = new GridStore db, 'mock.coffee', 'w', 
        metadata: 
          path: '/'
        content_type: 'application/coffee'
      gs.open (err) ->
        gs.writeFile __dirname + '/mock/mock.coffee', (err) ->
          gs.close next
      
          
  describe 'readfile', ->        
    it 'should read a file', (done) ->
      mfs.readfile '/folder/bar', {}, (err, meta) ->
        return done err if err
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
        return done err if err
        files.findOne
          filename: 'bar'
          'metadata.path': '/'
        , (err, doc) ->
          done err if err
          doc.should.exist
          done()
      stream.emit 'data', 'Hello'
      stream.emit 'end'
  
    it 'should reject if there is another file with the same name', (done) ->
      stream = new Stream()
      mfs.mkfile '/folder/bar', {stream}, (err, meta) ->
        fn = -> throw err if err
        fn.should.throw(/^File/)
        done()
      stream.emit 'data', 'Hello'
      stream.emit 'end'
  
  describe 'mkdir', ->
    it 'should create .empty file in new directory', (done) ->
      mfs.mkdir '/folder2', {}, (err, meta) ->
        return done err if err
        files.findOne
          'metadata.path': '/folder2'
        , (err, doc) ->
          done err if err
          doc.should.exist
          done()
        
    it 'should reject if there already is directory with the same name', (done) ->
      mfs.mkdir '/folder', {}, (err, meta) ->
        fn = -> throw err if err
        fn.should.throw(/^Directory/)
        done()
        
  describe 'rename', ->
    it 'should rename a file', (done) ->
      mfs.rename '/baz', {from:'/folder/bar'}, (err) ->
        return done err if err
        # Now there should be /baz file
        cursor = files.find 
          filename: 'baz'
          'metadata.path': '/'
        cursor.toArray (err, docs) ->
          return done err if err
          docs.should.not.be.empty
          # ... and /folder/bar should be gone
          cursor = files.find 
            filename: 'bar'
            'metadata.path': '/folder'
          cursor.toArray (err, docs) ->
            return done err if err
            docs.should.be.empty
            done()  
            
    it 'should rename a directory', (done) ->
      mfs.rename '/baz/', {from:'/folder/'}, (err) ->
        return done err if err
        # Now there should be file in /baz
        cursor = files.find 
          'metadata.path': '/baz'
        cursor.toArray (err, docs) ->
          return done err if err
          docs.should.not.be.empty
          # ... and /folder/ should be gone
          files.find 
            'metadata.path': '/folder'
          .toArray (err, docs) ->
            done err if err
            docs.should.be.empty
            done()  
  
  describe 'readdir', ->
    it 'should list all files and folders in directory', (done) ->
      mfs.readdir '/folder/', {}, (err, meta) ->
        return done err if err
        meta.should.have.property 'stream'
        stream = meta.stream
        data = []
        stream.on 'data', (chunk) -> data.push chunk
        stream.on 'end', ->
          data[0].should.be.a 'object'
          data[0].should.have.property 'name'
          data[0].should.have.property 'mime'
          data[0].should.have.property 'path'
          data[0].should.have.property 'size'
          data.should.have.length 3
          done()
          
  describe 'stat', ->
    it 'should return stat of a file', (done) ->
      mfs.stat '/folder/bar', {}, (err, meta) ->
        return done err if err
        meta.should.be.a 'object'
        meta.should.have.property 'name', 'bar'
        meta.should.have.property 'mime'
        meta.should.have.property 'path', '/folder'
        meta.should.have.property 'size'
        done()
          
    it 'should return stat of a directory', (done) ->
      mfs.stat '/folder/', {}, (err, meta) ->
        return done err if err
        meta.should.be.a 'object'
        meta.should.have.property 'name', 'folder'
        meta.should.have.property 'mime', 'inode/directory'
        meta.should.have.property 'path', '/'
        meta.should.have.property 'size', 3
        done()
          
    it 'should return error if the file or directory doesnt exist', (done) ->
      mfs.stat '/foobar', {}, (err, meta) ->
        fn = -> throw err if err
        fn.should.throw()
        done()
  
  describe 'copy', ->
    it 'should create copy of existing file', (done) ->
      mfs.copy '/folder/bar_copy', {from: '/mock.coffee'}, (err, meta) ->
        return done err if err
        files.findOne
          filename: 'bar_copy'
          'metadata.path': '/folder'
        , (err, doc) ->
          return done err if err
          doc.should.exist
          doc.should.be.a 'object'
          doc.should.have.property 'filename', 'bar_copy'
          doc.should.have.property 'metadata'
          doc.metadata.should.have.property 'path', '/folder'
          doc.should.have.property 'contentType', 'application/coffee'
          done()
          
  describe 'rmfile', ->
    it 'should remove file', (done) ->
      # Find the file to retrieve _id
      files.findOne
        filename: 'bar'
        'metadata.path': '/folder'
      , (err, doc) ->
        return done err if err
        # Remove the file
        mfs.rmfile '/folder/bar', {}, (err) ->
          return done err if err
          # No chunks connected to the file
          chunks.find
            files_id: doc._id
          .toArray (err, docs) -> 
            return done err if err
            docs.should.be.empty
            # The file itself should be gone
            files.findOne
              filename: 'bar'
              'metadata.path': '/folder'
            , (err, doc) ->
              return done err if err
              assert.equal doc, null
              done()
              
  describe 'rmdir', ->
    it 'should remove empty directory', (done) ->
      mfs.rmdir '/empty', {}, (err, meta) ->
        return done err if err
        files.findOne
          'metadata.path': '/empty'
        , (err, doc) ->
          return done err if err
          assert.equal doc, null
          done()
      
    it 'should not remove directory with files', (done) ->
      mfs.rmdir '/folder', {}, (err, meta) ->
        fn = -> throw err if err
        fn.should.throw(/not empty$/)
        files.find
          'metadata.path': /^\/folder/
        .toArray (err, docs) ->
          return done err if err
          docs.should.not.be.empty
          done()
        
    it 'should remove directory with files when recursive flag is true', (done) ->
      mfs.rmdir '/folder', {recursive: true}, (err, meta) ->
        done err if err
        files.findOne
          'metadata.path': /^\/folder/
        , (err, doc) ->
          return done err if err
          assert.equal doc, null
          done()
      
    