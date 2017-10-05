/***/
var NULL = null, TRUE = true, FALSE = false;
// 4-4-2013 [ystskm] create for ranked pool
var utils = require('./connection_utils'), inherits = require('util').inherits, net = require('net'), timers = require('timers'), EventEmitter = require('events').EventEmitter, inherits = require('util').inherits, Connection = require("./connection").Connection;

// Set processor, setImmediate if 0.10 otherwise nextTick
var processor = require('../utils').processor();

var util = require('util');
var DEBUG_CATEGORY = 'mongodb-native', DEBUG_KEY = 'db';
var FILE_NAME = __filename.split('/').slice(-1).toString();

var outLog = function() {
  var args = Array.prototype.slice.call(arguments);
  args.unshift(new Date().toGMTString() + ' - [' + FILE_NAME + '](' + process.pid + ')');
  console.log.apply(console, args);
};

// 10-28-2013 [ystskm] debug function set
var debug = process.env.NODE_DEBUG;
if(debug == 'true' || new RegExp(DEBUG_CATEGORY + '|' + DEBUG_KEY).test(String(debug))) {
  debug = outLog;
} else {
  debug = Function();
}
// <-- function debug() is available. <--

// 04-04-2013 [ystskm] add
var pid = 0;
var Pool = exports.Pool = function(manager, rank, size) {
  
  var p = this;
  EventEmitter.call(p);

  // "manager" is an instance of Class:ConnectionPool
  p.manager = manager, p.rank = rank;
  p.size = size, p.pool = NULL, p.ready = 0;
  
  // TODO remove
  p.pid = (pid += 1);
  
};
inherits(Pool, EventEmitter);

Pool.prototype.init = function() {
  var p = this;
  p.close();
  p.pool = [];
  p.createConnections();
};

Pool.prototype.seekAvailable = function() {
  var p = this;
  var len = p.pool.length, con;
  while(len--) {
    if((con = p.pool[len]).isFree()) 
      return con;
  }
};

Pool.prototype.createConnections = function() {

  var p = this, cp = p.manager;
  var size = p.size;
  var interval = Connection.CONNECTION_WAITING_TIMEOUT;
  debug('createConnections start. now:' + stat())

  if(p.pool.length == size) {
    if(p.is_reconnecting != NULL) {
      clearTimeout(p.is_reconnecting);
    }
    if(p.is_reconnecting == NULL || p.ready === 0) {
      delete p.is_reconnecting;
      p.ready = 1, p.emit('ready');
    } else {
      delete p.is_reconnecting;
      p.ready += 1, p.emit('reconnect', p.ready);
    }
    return;
  }

  // Create the associated connection
  var connection = new Connection(cp.connectionId++, cp.socketOptions);

  // Set logger on pool
  connection.logger = cp.logger;

  // Add connnect listener
  connection.on('connect', function() {

    var con = this;
    // // this.setEncoding("binary");
    /* 4-4-2013 TODO check commentou ok?
    this.setTimeout(0);
    this.setNoDelay();
    */

    // Update number of connected to server
    //      connectedTo = connectedTo + 1;
    // console.log('connected #' + connection.id + ', pool=' + p.pool.length);
    debug('connected #' + con.id);
    con.rank = p.rank;
    p.pool.push(con);
    next();

  });

  connection.on('error', function(e) {

    var con = this;
    debug('connect errored ' + p.pool.length + ', CON#' + con.id);
    mongoErrorOut(e);
    connection && connection.close();

  });

  connection.on('timeout', function(e) {
    // TODO check
    var con = this;
    debug('connect timeout ' + p.pool.length + ', CON#' + con.id);
    mongoErrorOut(e);

  });

  function mongoErrorOut(e) {
    util.error(e instanceof Error ? e: (e || {}).err ? e.err: e);
  }

  // Add a close listener
  connection.on('close', closeToReconnect);

  function closeToReconnect() {

    var con = this;
    // Only emit close once all connections in the pool have closed.
    // Note that connections which emit an error also emit a close.
    debug('connection closed. poolConnected.');
    outLog('A mongodb connection closed at rank=' + p.rank + '.');

    if(con.isConnected()) {
      return outLog('Wow!! This connection emits close in connected state.');
    }

    var idx = p.seek(connection);
    connection = NULL;

    if(idx == NULL) {
      return outLog('Not in pool close: ' + stat());
    }

    p.spliceOne(idx);
    if(p.is_reconnecting != NULL) {
      return;
    }

    // util.log('temporary, stop reconnect for debugging.');
    outLog('pool (pid=' + p.pid + ') have connection count: ' + stat());
    return;
    
    p.is_reconnecting = setTimeout(function() {
      outLog('Too much wait to reconnect.');
    }, interval);

    setTimeout(function() {
      outLog('Mongodb reconnecting starts.');
      next();
    }, 5000);

  }

  // TODO handle drain

  connection.on("message", function(message) {
    // capturing
    cp.emit('response', connection, function(requestId) {
      cp.emit('message', message, connection);
    });
  });

  connection.on("parseError", function(err) {
    // capturing
    cp.emit('response', connection, function(requestId) {
      cp.emit('parseError', err, requestId);
    });
  });

  connection.start();
  /*
  // Add the listener to the connection
  connection.addListener('data', receiveListener);
  */

  function stat() {
    return 'POOL#' + p.pid + ':' + [p.pool.length, p.size].join('/');
  }

  function next(err) {

    setImmediate ? setImmediate(function() {
      p.createConnections();
    }): setTimeout(function() {
      p.createConnections();
    }, 4);

  }

};

Pool.prototype.spliceOne = function(idx) {
  var p = this;
  var pool = p.pool;
  if(isArray(pool) && is('number', idx))
    pool.splice(idx, 1);
}

Pool.prototype.seek = function(con) {
  var p = this;
  var pool = p.pool;
  if(!isArray(pool)) {
    outLog('pool is not an array!')
    return NULL; 
  }
  var idx = pool.indexOf(con);
  return idx == -1 ? NULL: idx;
}

Pool.prototype.close = function() {
  var p = this;
  var pool = p.pool, con;
  if(!isArray(pool)) {
    return;
  }
  while(con = pool.shift())
    con.close();
};


//----------- //
function is(ty, x) {
  return typeof x == ty;
}
function isFunction(x) {
  return is('function', x);
}
function isArray(x) {
  return Array.isArray(x);
}
