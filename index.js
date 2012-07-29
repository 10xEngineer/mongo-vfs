var MongoFS = require('./mongofs').MongoFS;


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
 *   options.connString - MongoDB connection string (e.g. mongodb://localhost/)
 *   options.database - Database name
 *   options.bucketId - Bucket id
 *   options.bucketId - Bucket id
 *   options.bucketId - Bucket id
 */
module.exports = function setup(options) {

  var mongofs = new MongoFS(options);

  var vfs = {
    stat: stat
  }

  return vfs;

  function stat(path, callback) {
    if (!checkType([
      "path", path, "string"
    ], callback)) return;

    mongofs.getMetadata(path, callback);
  }
};