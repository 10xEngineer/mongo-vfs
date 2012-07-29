var vfs = require('../index')({});

describe('mongo-vfs', function() {
  describe('should have fs mgmt. function', function() {
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
    it('symlink', function() {
      var type = typeof(vfs.symlink);
      type.should.equal('function');
    });
    it('realpath', function() {
      var type = typeof(vfs.realpath);
      type.should.equal('function');
    });
    it('watch', function() {
      var type = typeof(vfs.watch);
      type.should.equal('function');
    });
    it('changedSince', function() {
      var type = typeof(vfs.changedSince);
      type.should.equal('function');
    });
  });
});
