try {
  exports.BSONPure = require('bson').BSONPure;
  exports.BSONNative = require('bson').BSONNative;
} catch(err) {
  // do nothing
}

// export the driver version
exports.version = require('../../package').version;

var files = [], p = '';

p = 'commands/';
files.push(p + 'base_command', p + 'db_command', p + 'delete_command', p
  + 'get_more_command', p + 'insert_command', p + 'kill_cursor_command', p
  + 'query_command', p + 'update_command');

p = '';
files.push('responses/mongo_reply', 'admin', 'collection',
  'connection/read_preference', 'connection/connection', 'connection/server',
  'connection/mongos', 'connection/repl_set/repl_set', 'cursor', 'db', 'mongo_client');

p = 'gridfs/';
files.push(p + 'grid', p + 'chunk', p + 'gridstore');

files.forEach(function(path) {
  	var module = require('./' + path);
  	for (var i in module) {
  		exports[i] = module[i];
    }
});

// backwards compat
exports.ReplSetServers = exports.ReplSet;
// Add BSON Classes
exports.Binary = require('bson').Binary;
exports.Code = require('bson').Code;
exports.DBRef = require('bson').DBRef;
exports.Double = require('bson').Double;
exports.Long = require('bson').Long;
exports.MinKey = require('bson').MinKey;
exports.MaxKey = require('bson').MaxKey;
exports.ObjectID = require('bson').ObjectID;
exports.Symbol = require('bson').Symbol;
exports.Timestamp = require('bson').Timestamp;  
// Add BSON Parser
exports.BSON = require('bson').BSONPure.BSON;

// Get the Db object
var Db = require('./db').Db;
// Set up the connect function
var connect = Db.connect;
var obj = connect;
// Map all values to the exports value
for(var name in exports) {
  obj[name] = exports[name];
}

// Add the pure and native backward compatible functions
exports.pure = exports.native = function() {
  return obj;
}

// Map all values to the exports value
for(var name in exports) {
  connect[name] = exports[name];
}

// Set our exports to be the connect function
module.exports = connect;
