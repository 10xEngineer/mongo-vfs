var _ = require('underscore');

var Connection = require('mongodb').Connection;
var MongoFS = require("./lib/mongofs");

// Simple type checking helper.
var checkType = function(vars, callback) {
  var errors, msg;
  if (callback == null) {
    callback = function() {};
  }
  errors = _.reject(vars, function(_var) {
    var name, validator, value;
    name = _var.name, value = _var.value, validator = _var.validator;
    return validator(value);
  });
  if (errors.length) {
    msg = (_.map(errors, function(err) {
      return "'" + err.name + "' - " + (typeof err.value);
    })).join('\n');
    callback(new TypeError(msg));
    return false;
  }
  return true;
};

/*
 * @options can have:
 *   options.host - database server host
 *   options.port - database server port
 *   options.database - Database name
 */
var setup = module.exports = function(options, cb) {
  options = _.defaults(options, {
    database: 'mongofs',
    host: 'localhost',
    port: Connection.DEFAULT_PORT
  });

  var mongofs = new MongoFS(options);
  
  var stat = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        val: _.isString
      }
    ], callback)) {
      return;
    }
    return mongofs.getMetadata(path, callback);
  };
  var readfile = function(path, options, callback) {
    return mongofs.readfile(path, options, callback);
  };
  var readdir = function(path, options, callback) {
    return mongofs.readdir(path, options, callback);
  };
  var mkfile = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        validator: _.isString
      }, {
        name: 'stream',
        value: options.stream,
        validator: _.isObject
      }
    ], callback)) {
      return;
    }
    return mongofs.mkfile(path, options, callback);
  };
  var mkdir = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        validator: _.isString
      }
    ], callback)) {
      return;
    }
    return mongofs.mkdir(path, options, callback);
  };
  var rmdir = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        validator: _.isString
      }
    ], callback)) {
      return;
    }
    return mongofs.rmdir(path, options, callback);
  };
  var rename = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        validator: _.isString
      }, {
        name: 'from',
        value: options.from,
        validator: _.isString
      }
    ], callback)) {
      return;
    }
    return mongofs.rename(path, options, callback);
  };
  var copy = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        validator: _.isString
      }
    ], callback)) {
      return;
    }
    return mongofs.copy(path, options, callback);
  };
  var rmFile = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        validator: _.isString
      }, {
        name: 'from',
        value: options.from,
        validator: _.isString
      }
    ], callback)) {
      return;
    }
    return mongofs.rmFile(path, options, callback);
  };
  
  var writeFile = function(path, options, callback) {
    if (!checkType([
      {
        name: 'path',
        value: path,
        validator: _.isString
      }
    ], callback)) {
      return;
    }
    return mongofs.writefile(path, options, callback);
  };
  
  var vfs = {
    stat: stat,
    readfile: readfile,
    readdir: readdir,
    writefile: writeFile,
    mkfile: mkfile,
    mkdir: mkdir,
    rmdir: rmdir,
    rmfile: rmFile,
    rename: rename,
    copy: copy,
    close: mongofs.close
  };
  
  return mongofs.open(function(err) {
    return cb(err, vfs);
  });
};

