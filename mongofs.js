var mongodb = require('mongodb');

var MongoFS = function(options) {
  this.options = options;

  // Initialize database, gridfs etc.
  this.db = new Db(options.database);
};

MongoFS.prototype.getMetadata = function(path, callback) {

};

module.exports.MongoFS = MongoFS;