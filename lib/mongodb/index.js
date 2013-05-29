try {
  exports.BSONPure = require('bson').BSONPure;
  exports.BSONNative = require('bson').BSONNative;
} catch(err) {
  // do nothing
}

var files = [], p = '';

p = 'commands/';
files.push(p + 'base_command', p + 'db_command', p + 'delete_command', p
  + 'get_more_command', p + 'insert_command', p + 'kill_cursor_command', p
  + 'query_command', p + 'update_command');

p = '';
files.push('responses/mongo_reply', 'admin', 'collection',
  'connection/read_preference', 'connection/connection', 'connection/server',
  'connection/mongos', 'connection/repl_set', 'cursor', 'db');

p = 'gridfs/';
files.push(p + 'grid', p + 'chunk', p + 'gridstore');

files.forEach(function(path) {
  var module = require('./' + path);
  for( var i in module) {
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

// Exports all the classes for the PURE JS BSON Parser
exports.pure = function() {
  var classes = {};
  // Map all the classes
  files.forEach(function(path) {
    var module = require('./' + path);
    for( var i in module) {
      classes[i] = module[i];
    }
  });

  // backwards compat
  classes.ReplSetServers = exports.ReplSet;

  // Add BSON Classes
  classes.Binary = require('bson').Binary;
  classes.Code = require('bson').Code;
  classes.DBRef = require('bson').DBRef;
  classes.Double = require('bson').Double;
  classes.Long = require('bson').Long;
  classes.MinKey = require('bson').MinKey;
  classes.MaxKey = require('bson').MaxKey;
  classes.ObjectID = require('bson').ObjectID;
  classes.Symbol = require('bson').Symbol;
  classes.Timestamp = require('bson').Timestamp;

  // Add BSON Parser
  classes.BSON = require('bson').BSONPure.BSON;

  // Return classes list
  return classes;
}

// Exports all the classes for the PURE JS BSON Parser
exports.native = function() {
  var classes = {};
  // Map all the classes
  files.forEach(function(path) {
    var module = require('./' + path);
    for( var i in module) {
      classes[i] = module[i];
    }
  });

  // Add BSON Classes
  classes.Binary = require('bson').Binary;
  classes.Code = require('bson').Code;
  classes.DBRef = require('bson').DBRef;
  classes.Double = require('bson').Double;
  classes.Long = require('bson').Long;
  classes.MinKey = require('bson').MinKey;
  classes.MaxKey = require('bson').MaxKey;
  classes.ObjectID = require('bson').ObjectID;
  classes.Symbol = require('bson').Symbol;
  classes.Timestamp = require('bson').Timestamp;

  // backwards compat
  classes.ReplSetServers = exports.ReplSet;

  // Add BSON Parser
  classes.BSON = require('bson').BSONNative.BSON;

  // Return classes list
  return classes;
}
