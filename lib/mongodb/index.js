const b = require('bson'); // => Buffer.alloc and Buffer.from will be substitued
try {
  exports.BSONPure = b.BSONPure;
  exports.BSONNative = b.BSONNative;
} catch(err) {
  // do nothing
}

// export the driver version
exports.version = require('../../package').version;

[ 'commands/base_command'
  , 'admin'
  , 'aggregation_cursor'
  , 'apm'
  , 'authenticate'
  , 'bulk/ordered'
  , 'bulk/unordered'
  , 'collection'
  , 'connection/read_preference'
  , 'connection/connection'
  , 'connection/server'
  , 'connection/mongos'
  , 'connection/repl_set/repl_set'
  , 'command_cursor'
  , 'cursor'
  , 'cursorstream'
  , 'db'
  , 'mongo_client'
  , 'scope'
  , 'timer'
  , 'gridfs/grid'
  ,	'gridfs/chunk'
  , 'gridfs/gridstore'].forEach(function (path) {
  	const module = require('./' + path);
  	for (let i in module) {
  		exports[i] = module[i];
    }
});

// backwards compat
exports.ReplSetServers = exports.ReplSet;
// Add BSON Classes
exports.Binary = b.Binary;
exports.Code = b.Code;
exports.DBRef = b.DBRef;
exports.Double = b.Double;
exports.Long = b.Long;
exports.MinKey = b.MinKey;
exports.MaxKey = b.MaxKey;
exports.ObjectID = b.ObjectID;
exports.Symbol = b.Symbol;
exports.Timestamp = b.Timestamp;  
// Add BSON Parser
exports.BSON = b.BSONPure ? b.BSONPure.BSON: b.BSON;

// Set up the connect function
const connect = exports.Db.connect;

// Add the pure and native backward compatible functions
exports.pure = exports.native = function() {
  return connect;
}

// Map all values to the exports value
for(let name in exports) {
  connect[name] = exports[name];
}

// Set our exports to be the connect function
module.exports = connect;
