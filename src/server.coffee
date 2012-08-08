root        = "http://localhost:8080/rest/"

stack       = require 'stack'
http        = require 'http'
httpAdapter = require 'vfs-http-adapter'
index       = require './index'

mongoReady = (err, vfs) ->
  throw err if err 
  server = http.createServer stack httpAdapter("/rest/", vfs, {})
  server.listen 8080 
  console.log("RESTful interface at " + root)
  
vfs = index 
  root: process.cwd() + "/"
  httpRoot: root
  , mongoReady

  