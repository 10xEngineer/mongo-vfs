var Connection = require('mongodb').Connection;

describe('mongo-vfs', function() {
  describe('should have', function(vfs) {
    var vfs;
    before(function(callback) {
      require('../index')({
        database: 'mongofs_test',
        host: 'localhost',
        port: Connection.DEFAULT_PORT
      }, function(error, vfsObj) {
        vfs = vfsObj;
        callback();
      });        
    });

    it('readfile', function() {
      var type = typeof(vfs.readfile);
      type.should.equal('function');
    });
    it('mkfile', function() {
      var type = typeof(vfs.mkfile);
      type.should.equal('function');
    });
    it('rmfile', function() {
      var type = typeof(vfs.rmfile);
      type.should.equal('function');
    });
    it('readdir', function() {
      var type = typeof(vfs.readdir);
      type.should.equal('function');
    });
    it('stat', function() {
      var type = typeof(vfs.stat);
      type.should.equal('function');
    });
    it('mkdir', function() {
      var type = typeof(vfs.mkdir);
      type.should.equal('function');
    });
    it('rmdir', function() {
      var type = typeof(vfs.rmdir);
      type.should.equal('function');
    });
    it('rename', function() {
      var type = typeof(vfs.rename);
      type.should.equal('function');
    });
    it('copy', function() {
      var type = typeof(vfs.copy);
      type.should.equal('function');
    });
    it('changedSince', function() {
      var type = typeof(vfs.changedSince);
      type.should.equal('function');
    });
    it('writeFile', function() {
      var type = typeof(vfs.writefile);
      type.should.equal('function');
    });

    after(function() {
      vfs.close();
    });
  });
});
