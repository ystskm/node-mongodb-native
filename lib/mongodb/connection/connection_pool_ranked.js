//4-4-2013 [ystskm] create for ranked pool
var utils = require('./connection_utils'),
  inherits = require('util').inherits,
  net = require('net'),
  timers = require('timers'),
  EventEmitter = require('events').EventEmitter,
  inherits = require('util').inherits,
  MongoReply = require("../responses/mongo_reply").MongoReply,
  Connection = require("./connection").Connection;

// Set processor, setImmediate if 0.10 otherwise nextTick
var processor = require('../utils').processor();

//10-28-2013 [ystskm] debug function set >>
var util = require('util'), debug = process.env.NODE_DEBUG;
var DEBUG_CATEGORY = 'mongodb-native', DEBUG_KEY = 'db', FILE_NAME = __filename.split('/').slice(-1).toString();
debug = debug && (debug == 'true' || (new RegExp(DEBUG_CATEGORY + '|' + DEBUG_KEY).test(debug))) ?
  function(s) { util.error(FILE_NAME + ': ' + s) }: function() {};
//<< function debug() is available.

//4-4-2013 [ystskm] add >>
var Pool = exports.Pool = function(manager, rank, size) {
  EventEmitter.call(this);
  // "manager" is an instance of Class:ConnectionPool
  this.manager = manager, this.rank = rank;
  this.size = size, this.pool = null, this.ready = 0;
};
inherits(Pool, EventEmitter);

Pool.prototype.init = function() {
  this.close(), this.pool = [], this.createConnections();
};

Pool.prototype.seekAvailable = function() {
  var len = this.pool.length, con;
  /* [ystskm] for pool debug
  var arr = this.pool.map(function(con){
    return parseInt(con.isFree());
  });
  console.log('[POOL rank: ' + this.rank + '] ' + arr);
  */
  while(len--)
    if((con = this.pool[len]).isFree())
      return con;
};

Pool.prototype.createConnections = function() {

  var self = this, cp = this.manager;
  var size = this.size;
  var interval = Connection.CONNECTION_WAITING_TIMEOUT;
  if(this.pool.length == size) {
    if(this.is_reconnecting != null)
      clearInterval(this.is_reconnecting);
    if(this.is_reconnecting == null || this.ready === 0) {
      delete this.is_reconnecting;
      this.ready = 1, this.emit('ready');
    } else {
      delete this.is_reconnecting;
      this.ready++, this.emit('reconnect', this.ready);
    }
    return;
  }

  // Create the associated connection
  var connection = new Connection(cp.connectionId++, cp.socketOptions);

  // Set logger on pool
  connection.logger = cp.logger;

  // Add connnect listener
  connection.on('connect', function() {

    // // this.setEncoding("binary");
    /* 4-4-2013 TODO check commentou ok?
    this.setTimeout(0);
    this.setNoDelay();
    */

    // Update number of connected to server
    //      connectedTo = connectedTo + 1;
    //console.log('connected #' + connection.id + ', pool=' + self.pool.length);
    debug('connected #' + connection.id);
    this.rank = self.rank;
    self.pool.push(this), next();

  });

  connection.on('error', function(err) {

    debug('connect errored #' + self.pool.length);
    mongoErrorOut(err), closeToReconnect();

  });

  connection.on('timeout', function(err) {
    // TODO check
    debug('connect timeout #' + self.pool.length);
    mongoErrorOut(err);

  });

  function mongoErrorOut(err) {
    util.error(err instanceof Error ? err: err && err.err ? err.err: err);
  }

  // Add a close listener
  connection.on('close', closeToReconnect);

  function closeToReconnect() {

    // Only emit close once all connections in the pool have closed.
    // Note that connections which emit an error also emit a close.
    debug('connection closed. poolConnected.');
    util.log('A mongodb connection closed at rank=' + self.rank + '.');
    self.spliceOne(self.seek(connection));
    connection.close(), delete connection;

    if(self.is_reconnecting != null)
      return;
    self.is_reconnecting = setInterval(next, interval);
    util.log('Mongodb reconnecting starts.');

  }

  // TODO handle drain

  connection.on("message", function(message) {
    // capturing
    cp.emit('response', connection), cp.emit('message', message);
  });

  connection.start();
  /*
  // Add the listener to the connection
  connection.addListener('data', receiveListener);
  */

  function next(err) {

    setImmediate ? setImmediate(function() {
      self.createConnections();
    }): setTimeout(function(){
      self.createConnections();
    });

  };

};

Pool.prototype.spliceOne = function(idx) {
  var pool = this.pool;
  if(Array.isArray(pool) && typeof idx == 'number')
    pool.splice(idx, 1);
}

Pool.prototype.seek = function(con) {
  var idx = null, pool = this.pool;
  if(Array.isArray(pool))
    for( var i = 0; i < pool.length; i++)
      if(pool[i] === con) {
        idx = i;
        break;
      }
  return idx;
}

Pool.prototype.close = function() {
  var pool = this.pool;
  if(Array.isArray(pool))
    pool.forEach(function(con) {
      con.close();
    });
};
// <<
