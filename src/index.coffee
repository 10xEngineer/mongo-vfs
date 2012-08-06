_           = require 'underscore'
Connection  = require('mongodb').Connection

# Simple type checking helper.
checkType = (vars, callback = ->) ->
  errors = _.reject vars, (_var) ->
    {name, value, validator} = _var
    validator value
  if errors.length
    msg = (_.map errors, (err) -> "'#{err.name}' - #{typeof err.value}").join '\n'
    callback new TypeError msg 
    return false
  true
  
MongoFS = require("./mongofs")

#
# * @options can have:
# *   options.connString - MongoDB connection string (e.g. mongodb://localhost/)
# *   options.database - Database name
# *   options.bucketId - Bucket id
# 
module.exports = setup = (options, cb) ->
  options = _.defaults options, 
    database: 'mongofs'
    host: 'localhost'
    port: Connection.DEFAULT_PORT
    
  mongofs = new MongoFS options
  
  stat = (path, options, callback) ->
    return unless checkType [
      name: 'path', value: path, val: _.isString
    ], callback 
    mongofs.getMetadata path, callback
  
  readfile = (path, options, callback) ->
    mongofs.readfile path, options, callback  
  
  readdir = (path, options, callback) ->
    mongofs.readdir path, options, callback
  
  mkfile = (path, options, callback) ->
    return unless checkType [
      name: 'path', value: path, validator: _.isString
    , name: 'stream', value: options.stream, validator: _.isObject
    ], callback 
    mongofs.mkfile path, options, callback
  
  mkdir = (path, options, callback) ->
    return unless checkType [
      name: 'path', value: path, validator: _.isString
    ], callback 
    mongofs.mkdir path, options, callback
    
  rmdir = (path, options, callback) ->
    return unless checkType [
      name: 'path', value: path, validator: _.isString
    ], callback 
    mongofs.rmdir path, options, callback
  
  vfs = 
    stat: stat
    readfile: readfile
    readdir: readdir    
    mkfile: mkfile    
    mkdir: mkdir    
    rmdir: rmdir    
    
  mongofs.open (err) ->
    cb err, vfs