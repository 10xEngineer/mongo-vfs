var _ = require('underscore');

var Connection = require('mongodb').Connection;
var MongoFS = require("./lib/mongofs");

// Simple type checking helper.
function checkType(vars, callback) {
  if (typeof callback !== "function") {
    throw new TypeError("Please pass in a function for the callback");
  }
  var errors = [];
  for (var i = 0, l = vars.length; i < l; i += 3) {
    var name = vars[i];
    var value = vars[i + 1];
    var expectedType = vars[i + 2];
    var actualType = value === null ? "null" : Array.isArray(value) ? "array" : typeof value;
    if (actualType !== expectedType) {
      errors += "Expected " + name + " to be " + expectedType + " but was " + actualType;
    }
  }
  if (errors.length) {
    callback (new TypeError(errors.join("\n")));
    return false;
  }
  return true;
}

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
  
  var vfs = {
    stat: stat,
    readfile: readfile,
    readdir: readdir,
    mkfile: mkfile,
    mkdir: mkdir,
    rmdir: rmdir,
    close: mongofs.close
  };
  
  return mongofs.open(function(err) {
    return cb(err, vfs);
  });
};

