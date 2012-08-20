(function() {
  var mime = require('mime');
  var Path = require('path');
  var _ = require('underscore');
  var EventEmitter = require('events').EventEmitter;
  var Stream = require('stream');
  var async = require('async');
  
  var mongodb = require('mongodb');
  var Db = mongodb.Db;
  var Server = mongodb.Server;
  var Connection = mongodb.Connection;
  var GridStore = mongodb.GridStore;

  var MongoFS = (function() {
    var files;
    var chunks;
    var db;

    var extractName = function(path) {
      return [addTrailingSlash(Path.dirname(path)), Path.basename(path)];
    };

    var escape = function(path) {
      return path.replace(/\//g, '\\\/');
    };

    var stripTrailingSlash = function(path) {
      if ((path.length !== 1) && path.charAt(path.length - 1) === '/') {
        path = path.slice(0, -1);
      }
      return path;
    };

    var addTrailingSlash = function(path) {
      if (path.charAt(path.length - 1) !== '/') {
        path += '/';
      }
      return path;
    };

    var createStatEntry = function(doc) {
      var href;
      href = addTrailingSlash(doc.metadata.path) + doc.filename;
      return {
        name: doc.filename,
        mime: doc.contentType,
        path: addTrailingSlash(doc.metadata.path),
        href: href,
        size: doc.length
      };
    };

    var exist = {
      file: function(path, options, cb) {
        var base, dir, _ref;
        if (cb == null) {
          cb = function() {};
        }
        _ref = extractName(path), dir = addTrailingSlash(_ref[0]), base = _ref[1];
        return files.findOne({
          filename: base,
          'metadata.path': dir,
          'metadata.bucket': options.bucketId
        }, function(err, doc) {
          if (err) {
            return cb(err);
          }
          if (doc) {
            return cb(null, doc);
          } else {
            return cb();
          }
        });
      },
      folder: function(path, options, cb) {
        var base, dir, _ref;
        if (cb == null) {
          cb = function() {};
        }
        _ref = extractName(stripTrailingSlash(path)), dir = addTrailingSlash(_ref[0]), base = _ref[1];
        return iterateThroughFolder(path, options, function(err, docs) {
          var res;
          if (err) {
            return cb(err);
          }
          res = {
            filename: base,
            contentType: 'inode/directory',
            length: docs.length,
            metadata: {
              path: dir
            }
          };
          return cb(null, res);
        });
      }
    };

    var iterateThroughFolder = function(path, options, cb) {
      var base, dir, subFiles, subFolders, _ref;
      if (cb == null) {
        cb = function() {};
      }
      _ref = extractName(path = stripTrailingSlash(path)), dir = addTrailingSlash(_ref[0]), base = _ref[1];
      subFiles = function(next) {
        return files.find({
          filename: {
            $ne: '.empty'
          },
          'metadata.path': addTrailingSlash(path),
          'metadata.bucket': options.bucketId
        }).toArray(next);
      };
      subFolders = function(next) {
        var regex = new RegExp("^" + escape(addTrailingSlash(path)) + "\\w+\\/$");
        return files.find({
          filename: '.empty',
          'metadata.path': regex,
          'metadata.bucket': options.bucketId
        }).toArray(function(err, docs) {
          if (err) {
            return next(err);
          }
          if (_.isArray(docs) && !_.isEmpty(docs)) {
            docs = _.uniq(docs, false, function(doc) {
              return doc.metadata.path;
            });
            docs = _.map(docs, function(doc) {
              var _ref1;
              _ref1 = extractName(doc.metadata.path), dir = addTrailingSlash(_ref1[0]), base = stripTrailingSlash(_ref1[1]);
              doc.filename = base;
              doc.metadata.path = dir;
              doc.contentType = 'inode/directory';
              return doc;
            });
          }
          return next(null, docs);
        });
      };
      return async.parallel([subFiles, subFolders], function(err, results) {
        if (err) {
          return cb(err);
        }
        return cb(null, Array.prototype.concat.apply([], results));
      });
    };

    function MongoFS(options) {
      var server;
      this.options = options;
      server = new Server(this.options.host, this.options.port, {});
      db = new Db(this.options.database, server);
    }

    MongoFS.prototype.open = function(cb) {
      return db.open(function(err, db) {
        db = db;
        files = db.collection('fs.files');
        chunks = db.collection('fs.chunks');
        return cb.apply(null, arguments);
      });
    };

    MongoFS.prototype.close = function() {
      db.close();
    };

    MongoFS.prototype.readdir = function(path, options, cb) {
      var stream;
      options.bucketId = options.bucketId || this.options.bucketId;
      stream = new Stream();
      stream.readable = true;
      cb(null, {
        stream: stream
      });
      return iterateThroughFolder(path, options, function(err, docs) {
        if (err) {
          return stream.emit('error', err);
        }
        var iterator = function(doc, next) {
          var createStatsAndEmit, subFolderPath;
          createStatsAndEmit = function() {
            stream.emit('data', createStatEntry(doc));
            return next();
          };
          if (doc.contentType === 'inode/directory') {
            subFolderPath = addTrailingSlash(doc.metadata.path) + doc.filename;
            return exist.folder(subFolderPath, options, function(err, _doc) {
              doc = _doc;
              return createStatsAndEmit();
            });
          } else {
            return createStatsAndEmit();
          }
        };
        return async.forEach(docs, iterator, function(err) {
          if (err) {
            return stream.emit('error', err);
          }
          return stream.emit('end');
        });
      });
    };

    MongoFS.prototype.readfile = function(path, options, cb) {
      var base, dir, query, stream, _ref;
      options.bucketId = options.bucketId || this.options.bucketId;
      _ref = extractName(path), dir = addTrailingSlash(_ref[0]), base = _ref[1];
      stream = new Stream();
      query = {
        filename: base,
        'metadata.path': dir,
        'metadata.bucket': options.bucketId
      };
      return files.findOne(query, function(err, doc) {
        var cursor;
        if (err) {
          return cb(err, {});
        }
        if (!doc) {
          return cb(err, {});
        }
        cb(null, {
          stream: stream,
          mime: doc.contentType,
          bucket: doc.metadata.bucket
        });
        cursor = chunks.find({
          files_id: doc._id
        }, ['data']);
        return cursor.each(function(err, chunk) {
          if (err) {
            return stream.emit('error', err);
          }
          if (!chunk) {
            return stream.emit('end');
          }
          return stream.emit('data', chunk.data.buffer);
        });
      });
    };

    MongoFS.prototype.writefile = function(path, options, cb) {
      var base, buffer, dir, gs, meta, onData, onEnd, readable, temp, that, _ref;
      options.bucketId = options.bucketId || this.options.bucketId;
      that = this;
      _ref = extractName(path), dir = addTrailingSlash(_ref[0]), base = _ref[1];
      buffer = [];
      meta = {};

      options.mime = options.mime || mime.lookup(path);
      (readable = options.stream).on('data', onData = function(chunk) {
        return buffer.push(['data', chunk]);
      });
      readable.on('end', onEnd = function() {
        return buffer.push(['end']);
      });
      gs = new GridStore(db, base, 'w', {
        content_type: options.mime,
        metadata: {
          path: dir,
          bucket: options.bucketId
        }
      });
      return gs.open(function(err) {
        readable.on('data', function(chunk) {
          return gs.write(chunk, function(err) {});
        });
        readable.on('end', function() {
          return gs.close(cb);
        });
        readable.removeListener('data', onData);
        readable.removeListener('end', onEnd);
        return _.forEach(buffer, function(event) {
          return readable.emit.apply(readable, event);
        });
      });
    };

    MongoFS.prototype.mkfile = function(path, options, cb) {
      var base, buffer, dir, gs, meta, onData, onEnd, readable, temp, that, _ref;
      options.bucketId = options.bucketId || this.options.bucketId;
      that = this;
      _ref = extractName(path), dir = addTrailingSlash(_ref[0]), base = _ref[1];
      buffer = [];
      meta = {};
      options.mime = options.mime || mime.lookup(path);
      (readable = options.stream).on('data', onData = function(chunk) {
        return buffer.push(['data', chunk]);
      });
      readable.on('end', onEnd = function() {
        return buffer.push(['end']);
      });
      temp = "" + base + ".";
      temp += "" + (Date.now().toString(36)) + "-";
      temp += "" + ((Math.random() * 0x100000000).toString(36));
      gs = new GridStore(db, temp, 'w', {
        content_type: options.mime,
        metadata: {
          path: dir,
          bucket: options.bucketId
        }
      });
      return gs.open(function(err) {
        readable.on('data', function(chunk) {
          return gs.write(chunk, function(err) {});
        });
        readable.on('end', function() {
          var rename;
          rename = function() {
            return exist.file(path, options, function(err, doc) {
              if (doc) {
                return cb(new Error('File already exists'));
              } else {
                options.from = dir + temp;
                return that.rename(path, options, function(err) {
                  return cb(err, meta);
                });
              }
            });
          };
          return gs.close(rename);
        });
        readable.removeListener('data', onData);
        readable.removeListener('end', onEnd);
        return _.forEach(buffer, function(event) {
          return readable.emit.apply(readable, event);
        });
      });
    };

    MongoFS.prototype.mkdir = function(path, options, cb) {
      var base, dir, _ref,
        _this = this;
      options.bucketId = options.bucketId || this.options.bucketId;
      _ref = extractName(path = stripTrailingSlash(path)), dir = stripTrailingSlash(_ref[0]) + '/', base = _ref[1];
      return iterateThroughFolder(dir, options, function(err, docs) {
        var stream;
        if (err) {
          return cb(err);
        }
        if (_.any(docs, function(doc) {
          return (doc.contentType === 'inode/directory') && doc.filename === base;
        })) {
          return cb(new Error("Directory " + path + " already exists"));
        }
        var stream = options.stream = new Stream();
        stream.readable = true;
        _this.mkfile(addTrailingSlash(path) + '.empty', options, cb);
        return stream.emit('end');
      });
    };

    MongoFS.prototype.copy = function(path, options, cb) {
      var _this = this;
      options.bucketId = options.bucketId || this.options.bucketId;
      return this.readfile(options.from, options, function(err, readMeta) {
        if (err) {
          return cb(err);
        }
        readMeta.bucketId = readMeta.bucket;
        return _this.mkfile(path, readMeta, function(err, writeMeta) {
          return cb(err, writeMeta);
        });
      });
    };

    MongoFS.prototype.rename = function(path, options, cb) {
      var from = extractName(stripTrailingSlash(options.from));
      var to = extractName(stripTrailingSlash(path));
      options.bucketId = options.bucketId || this.options.bucketId;
      var renameFile = function(next) {
        return files.findAndModify({
          filename: from[1],
          'metadata.path': from[0],
          'metadata.bucket': options.bucketId
        }, [], {
          $set: {
            filename: to[1],
            'metadata.path': to[0]
          }
        }, {}, next);
      };
      var renameFolder = function(next) {
        path = stripTrailingSlash(path);
        return files.update({
          'metadata.path': options.from,
          'metadata.bucket': options.bucketId
        }, {
          $set: {
            'metadata.path': addTrailingSlash(path)
          }
        }, {
          multi: true,
          safe: true
        }, next);
      };
      var command;
      if(path.charAt(path.length-1) == '/') {
        command = renameFolder;
      } else {
        command = renameFile;
      }
      command(cb);
    };

    MongoFS.prototype.stat = function(path, options, cb) {
      var searchForDir, searchForFile;
      options.bucketId = options.bucketId || this.options.bucketId;
      searchForDir = function() {
        return exist.folder(path, options, function(err, dir) {
          if (err) {
            return cb(err);
          }
          if (!(dir.length > 0)) {
            return cb(new Error('File does not exist'));
          } else {
            return cb(null, createStatEntry(dir));
          }
        });
      };
      searchForFile = function() {
        return exist.file(path, options, function(err, doc) {
          if (err) {
            return cb(err);
          }
          if (!doc) {
            return searchForDir();
          } else {
            return cb(null, createStatEntry(doc));
          }
        });
      };
      return searchForFile();
    };

    MongoFS.prototype.rmfile = function(path, options, cb) {
      var base, dir, _ref;
      options.bucketId = options.bucketId || this.options.bucketId;
      _ref = extractName(path), dir = addTrailingSlash(_ref[0]), base = _ref[1];
      return files.findOne({
        filename: base,
        'metadata.path': dir,
        'metadata.bucket': options.bucketId
      }, function(err, doc) {
        if (err) {
          return cb(err);
        }
        return files.remove({
          _id: doc._id
        }, function(err) {
          if (err) {
            return cb(err);
          }
          return _.defer(function() {
            return chunks.remove({
              files_id: doc._id
            }, function(err) {
              if (err) {
                return cb(err);
              }
              return _.defer(cb);
            });
          });
        });
      });
    };

    MongoFS.prototype.rmdir = function(path, options, cb) {
      options.bucketId = options.bucketId || this.options.bucketId;
      path = addTrailingSlash(path);
      var _ref;
      if (options == null) {
        options = {};
      }
      if ((_ref = options.recursive) == null) {
        options.recursive = false;
      }
      return files.find({
        'metadata.path': path,
        'metadata.bucket': options.bucketId
      }).toArray(function(err, docs) {
        var empty;
        empty = (docs.length === 1) && (docs[0].filename === '.empty') && (docs[0].length === 0);
        if (options.recursive || empty) {
          return files.remove({
            'metadata.path': new RegExp('^' + path)
          }, function(err) {
            return _.defer(cb, err);
          });
        } else {
          return cb(new Error('Directory is not empty'));
        }
      });
    };

    return MongoFS;

  })();

  module.exports = MongoFS;

}).call(this);
