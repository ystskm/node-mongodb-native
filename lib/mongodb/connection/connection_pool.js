/***/
var NULL = null, TRUE = true, FALSE = false;
// 4-4-2013 [ystskm] modified for using ranked pool
var utils = require('./connection_utils'), inherits = require('util').inherits, net = require('net'), EventEmitter = require('events').EventEmitter, inherits = require('util').inherits, Connection = require("./connection").Connection, Pool = require("./connection_pool_ranked").Pool;

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

// 4-4-2013 [ystskm] add
// singleton pools for exit
var ConnectionPools = [];

// 4-4-2013 [ystskm] add
// callback target pool
var notReplied = {};

// 4-4-2013 [ystskm] add
// Add close handler on process exit
process.on('exit', function() {
  ConnectionPools.forEach(function(cp) {
    cp.stop();
  });
});

// 4-4-2013 [ystskm] add for debug
// var coid = 0;

// 4-4-2013 [ystskm] add for debug
// var wrid = 0;

var ConnectionPool = exports.ConnectionPool = function(host, port, poolSize,
  bson, socketOptions) {

  if(!is('string', host) || !is('number', port)) {
    throw "host and port must be specified [" + host + ":" + port + "]";
  }

  //[ystskm]
  var cp = this ,self = cp;
  cp.host = host, cp.port = port;

  // Set up event emitter
  EventEmitter.call(cp);
  
  // Keep all options for the socket in a specific collection allowing the user to specify the
  // Wished upon socket connection parameters
  cp.socketOptions = is('object', socketOptions) ? socketOptions || {}: {};
  cp.socketOptions.host = host;
  cp.socketOptions.port = port;
  cp.bson = bson;

  // PoolSize is always + 1 for special reserved "measurment" socket (like ping, stats etc)
  if (!is('number', poolSize)) {
    poolSize = parseInt(poolSize.toString(), 10);
    if (isNaN(poolSize)) {
      throw new Error("poolSize must be a number!");
    }
  }

  cp.poolSize = poolSize;
  cp.minPoolSize = Math.floor(cp.poolSize / 2) + 1;

  // Set default settings for the socket options
  utils.setIntegerParameter(cp.socketOptions, 'timeout', 0);
  // Delay before writing out the data to the server
  utils.setBooleanParameter(cp.socketOptions, 'noDelay', TRUE);
  // Delay before writing out the data to the server
  utils.setIntegerParameter(cp.socketOptions, 'keepAlive', 0);
  // Set the encoding of the data read, default is binary == null
  utils.setStringParameter(cp.socketOptions, 'encoding', NULL);
  // Allows you to set a throttling bufferSize if you need to stop overflows
  utils.setIntegerParameter(cp.socketOptions, 'bufferSize', 0);

  // Internal structures
  cp.openConnections = [];

  // [ystskm]
  _renewalPools(cp);

  // [ystskm] send waiting queues.
  cp.queues = [];

  // Assign connection id's
  cp.connectionId = 0;

  // Current index for selection of pool connection
  cp.currentConnectionIndex = 0;
  // The pool state
  cp._poolState = ConnectionPool.State.Disconnect;
  // timeout control
  cp._timeout = FALSE;

  // [ystskm]
  ConnectionPools.push(cp);

  // [ystskm]
  // callback = function() { 
  //   cp.emit('message', message); // => propagate message to "server.js" 
  // }
  cp.on('response', function(con, callback) {
    var store = con.store || '';
    /* for debug 
    console.log('[connection_pool.js] return requestId: '
      + (store && store.requestId));
    */
    if(store)
      store.clearRequest();
    /* for debug
    console.log(Object.keys(notReplied));
    */
    callback(store.requestId);
  });

}

inherits(ConnectionPool, EventEmitter);

// [ystskm]
ConnectionPool.prototype.forEachConnection = function(fn){
  this.pools.forEach(function(p){
    p.pool.forEach(fn);
  });
}

ConnectionPool.prototype.setMaxBsonSize = function(maxBsonSize) {
  
  var cp = this;
  if(maxBsonSize == NULL){
    maxBsonSize = Connection.DEFAULT_MAX_BSON_SIZE;
  }

  // [ystskm]
  cp.forEachConnection(function(con){
    con.maxBsonSize = maxBsonSize;
    con.maxBsonSettings.maxBsonSize = maxBsonSize;
  });
//  for(var i = 0; i < this.openConnections.length; i++) {
//    this.openConnections[i].maxBsonSize = maxBsonSize;
//    this.openConnections[i].maxBsonSettings.maxBsonSize = maxBsonSize;
//  }

};

ConnectionPool.prototype.setMaxMessageSizeBytes = function(maxMessageSizeBytes) {
  
  var cp = this;
  if(maxMessageSizeBytes == NULL){
    maxMessageSizeBytes = Connection.DEFAULT_MAX_MESSAGE_SIZE;
  }

  // [ystskm]
  cp.forEachConnection(function(con){
    con.maxMessageSizeBytes = maxMessageSizeBytes;
    con.maxBsonSettings.maxMessageSizeBytes = maxMessageSizeBytes;
  });
//  for(var i = 0; i < this.openConnections.length; i++) {
//    this.openConnections[i].maxMessageSizeBytes = maxMessageSizeBytes;
//    this.openConnections[i].maxBsonSettings.maxMessageSizeBytes = maxMessageSizeBytes;
//  }

};

ConnectionPool.prototype.setMaxWriteBatchSize = function(maxWriteBatchSize) {
  
  var cp = this;
  if(maxWriteBatchSize == NULL){
    maxWriteBatchSize = Connection.DEFAULT_MAX_WRITE_BATCH_SIZE;
  }

  // [ystskm]
  cp.forEachConnection(function(con){
    con.maxWriteBatchSize = maxWriteBatchSize;
  });
//  for(var i = 0; i < this.openConnections.length; i++) {
//    this.openConnections[i].maxWriteBatchSize = maxWriteBatchSize;
//  }

};

// Start a function
var _renewalPools = function(cp) {
  cp.pools = [].concat(cp.poolSize).map(function(num, i) {
    return new Pool(cp, i, parseInt(num) || 1);
  });
};
var _connect = function(cp) {
  var _self = cp;
  return new function() {
    /* 4-4-2013 [ystskm] change to use "pool_ranked"
     * ConnectionPool Class is using only management.
    // Create a new connection instance
    var connection = new Connection(_self.connectionId++, _self.socketOptions);
    // Set logger on pool
    connection.logger = _self.logger;
    // Connect handler
    connection.on("connect", function(err, connection) {
      // Add connection to list of open connections
      _self.openConnections.push(connection);
      // If the number of open connections is equal to the poolSize signal ready pool
      if(_self.openConnections.length === _self.poolSize && _self._poolState !== 'disconnected') {
        // Set connected
        _self._poolState = 'connected';
        // Emit pool ready
        _self.emit("poolReady");
      } else if(_self.openConnections.length < _self.poolSize) {
        // We need to open another connection, make sure it's in the next
        // tick so we don't get a cascade of errors
        process.nextTick(function() {
          _connect(_self);
        });
      }
    });
    */
    // [ystskm] -->
    var len = cp.pools.length;
    // set ready event for eack rank pool
    cp.pools.forEach(function(pool, rank) {
      
      // Set ready / reconnect handler, go connecting
      pool.once('ready', onRankPoolReady).on('reconnect', onRankPoolReconnect);
      pool.init();
      
      function onRankPoolReady() {
        outLog('pool ready for ' + cp.host + ':' + cp.port + ' (rank:' + pool.rank + ', size:' + pool.size + ', wait:' + (len - 1) + ')');
        if(--len != 0) {
          return;
        }
        cp._poolState = ConnectionPool.State.Connected;
        cp.emit(ConnectionPool.Event.Ready);
      }

      function onRankPoolReconnect(n) {
        outLog('pool reconnect for ' + cp.host + ':' + cp.port + ' (rank:' + pool.rank + ', size:' + pool.size + ', times:' + n + ')');
        if(--len != 0) {
          return;
        }
        cp._poolState = ConnectionPool.State.Connected;
        cp.emit(ConnectionPool.Event.Reconnect);
      }
      
    });
    // <--
    /*
    var numberOfErrors = 0

    // Error handler
    connection.on("error", function(err, connection) {
      numberOfErrors++;
      // If we are already disconnected ignore the event
      if(_self._poolState != 'disconnected'
        && _self.listeners("error").length > 0) {
        _self.emit("error", err);
      }

      // Set disconnected
      _self._poolState = 'disconnected';
      // Stop
      _self.stop();
    });

    // Close handler
    connection.on("close", function() {
      // If we are already disconnected ignore the event
      if(_self._poolState !== 'disconnected'
        && _self.listeners("close").length > 0) {
        _self.emit("close");
      }
      // Set disconnected
      _self._poolState = 'disconnected';
      // Stop
      _self.stop();
    });

    // Timeout handler
    connection.on("timeout", function(err, connection) {
      // If we are already disconnected ignore the event
      if(_self._poolState !== 'disconnected'
        && _self.listeners("timeout").length > 0) {
        _self.emit("timeout", err);
      }
      // Set disconnected
      _self._poolState = 'disconnected';
      // Stop
      _self.stop();
    });

    // Parse error, needs a complete shutdown of the pool
    connection.on("parseError", function() {
      // If we are already disconnected ignore the event
      if(_self._poolState !== 'disconnected'
        && _self.listeners("parseError").length > 0) {
        _self.emit("parseError", new Error("parseError occured"));
      }

      _self.stop();
    });

    connection.on("message", function(message) {
      _self.emit("message", message);
    });

    // Start connection in the next tick
    connection.start();
    */
  }();
}

//[ystskm]
ConnectionPool.prototype.enqueue = function(rank, fn) {
  
  if(isFunction(rank))
    fn = rank, rank = 0;
  
  var Q = this.queues;
  if(!isArray(Q[rank]))
    Q[rank] = [];
  Q[rank].push(fn);
  
};

//[ystskm]
ConnectionPool.prototype.dequeue = function(rank) {
  
  var Q = this.queues, i, q;
  for(i = rank; i >= 0; i--) {
    q = Q[rank] || [];
    if(q.length)
      return q.shift()();
  }

};

// Start method, will throw error if no listeners are available
// Pass in an instance of the listener that contains the api for
// finding callbacks for a given message etc.
ConnectionPool.prototype.start = function() {
  
  var cp = this, self = cp;
  var markerDate = new Date().getTime();

  // [ystskm] duplicated
  if(cp.isConnecting()) {
    return;
  }
  if(cp.isConnected()) {
    return cp.emit(ConnectionPool.Event.Ready);
  }

  if(cp.listeners(ConnectionPool.Event.Ready).length == 0) {
    throw "pool must have at least one listener ready that responds to the [" + ConnectionPool.Event.Ready + "] event";
  }

  // Set pool state to connecting
  cp._poolState = ConnectionPool.State.Connecting;
  cp._timeout = FALSE;
  _connect(cp);
  
}

// Restart a connection pool (on a close the pool might be in a wrong state)
ConnectionPool.prototype.restart = function(intv) {
  
  var cp = this;
  // Close all connections
  cp.stop(FALSE);
  // Now restart the pool
  setTimeout(function(){ cp.start(); }, intv || 3000);
  // outLog('ConnectionPool.prototype.restart successful.', cp.isConnected(), cp.isConnecting(), cp.isDisconnect());
  
}

// Stop the connections in the pool
ConnectionPool.prototype.stop = function(removeListeners) {
  
  var cp = this;
  var Q = cp.queues, i;
  removeListeners = removeListeners == NULL ? TRUE: removeListeners;
  
  if(cp.isConnecting()) {
    outLog('WARN:ConnectionPool is connecting state but requested stop.');
  }
  
  // Set disconnected
  cp._poolState = ConnectionPool.State.Disconnect;

  // Exhaust all request
  var cb;
  if(isArray(Q)) { 
    Q.forEach(function(q){
      if(!isArray(q)) return;
      while(cb = q.shift()) try { cb(new Error('ConnectionPool stopped.')); } catch(e) { console.log('Callback Exception:', e); }
    });
  }
  
  // Clear all queue callback
  cp.queues = [];
  
  // Clear all listeners if specified
  if(removeListeners) {
    cp.removeAllEventListeners();
  }

  // Close all connections
  // [ystskm] "openConnections" may always empty
  for( i = 0; i < cp.openConnections.length; i++) {
    cp.openConnections[i].close();
  }

  // Clean up
  cp.openConnections = [];

  // [ystskm]
  for( i = 0; i < cp.pools.length; i++) {
    cp.pools[i].close();
  }

  // [ystskm]
  _renewalPools(cp);
  // outLog('ConnectionPool.prototype.stop successful. removeListeners:' + removeListeners, cp.isConnected(), cp.isConnecting(), cp.isDisconnect());

}

// Check the status of the connection
ConnectionPool.prototype.isConnected = function() {
  return this._poolState === ConnectionPool.State.Connected;
};
ConnectionPool.prototype.isConnecting = function() {
  return this._poolState === ConnectionPool.State.Connecting;
};
ConnectionPool.prototype.isDisconnect = function() {
  return this._poolState === ConnectionPool.State.Disconnect;
};

// Checkout a connection from the pool for usage, or grab a specific pool instance
ConnectionPool.prototype.checkoutConnection = function(id) {
  /*
  var index = (this.currentConnectionIndex++ % (this.openConnections.length));
  var connection = this.openConnections[index];
  return connection;
  */
}

// [ystskm]
//Checkout a connection from the pool with rank
ConnectionPool.prototype.checkoutRankConnection = function() {
  // using connection must be elected just before write.
  return this;
}

// 6-4-2014 [ystskm]
// add method for v1.4.0
ConnectionPool.prototype.isCompatible = function(){
  
  var cp = this;
  if(cp.serverCapabilities == NULL) return TRUE;
  // Is compatible with backward server
  if(cp.serverCapabilities.minWireVersion == 0 
    && cp.serverCapabilities.maxWireVersion ==0) return TRUE;

  // Check if we overlap
  if(cp.serverCapabilities.minWireVersion >= cp.minWireVersion
    && cp.serverCapabilities.maxWireVersion <= cp.maxWireVersion) return TRUE;

  // Not compatible
  return FALSE;
  
}

// [ystskm]
ConnectionPool.prototype.write = function(commands, options, callback) {

  var cp = this, con, requestId;
  options = options || {};

  var rank = options.rank || 0;
  if(cp.isConnected()) {
    // outLog('writeCommands(' + rank + '):isConnected');
    writeCommands();
    // => write or enqueue commands
  } else {
    // outLog('writeCommands(' + rank + '):'+ cp._poolState);
    cp.once(ConnectionPool.Event.Ready, writeCommands);
    cp.isConnecting() || cp.start();
  } 

  function writeCommands() {
    // outLog('writeCommands(' + rank + '):2.0');
    try {
  
      requestId = [].concat(commands).pop().getRequestId();
      con = cp.seekConnection(rank);
  
      if(!con) {
        return cp.enqueue(rank, function(er) {
          
          if(er) {
            return callback(er);
          }
          
          if(cp.isConnected()) {
            cp.write(commands, options, callback);
          } else {
            callback(new Error('Connection not established yet.'));
          }
          
        });
      }
  
      notReplied[requestId] = con.store = {
        requestId: requestId,
        time: Date.now(),
        clearRequest: clearRequest,
        commands: commands
      };
  
      /* for debug
      console.log('[request] id: ' + requestId + ' (async:' + options.async + ')');
      console.log('[request] command: ' + Object.keys(commands));
      if(commands.query)
        console.log('[request] ' + JSON.stringify(commands.query));
      if(commands.selector)
        console.log('[request] ' + JSON.stringify(commands.selector));
      */
  
      //  debug(function() {
      //  var comm = Array.isArray(commands) ? commands[0]: commands;
      //  return 'reqId:' + requestId + ', conId:' + con.id + ', header:'
      //    + comm.collectionName + ', ' + JSON.stringify(comm.query);
      //});
  
      con.write(commands, callback);
  
      if(commands.query || Array.isArray(commands) || options.async) {
        return; // async event will emit from connection
      }
  
      clearRequest();
      
    } catch(e) {
      util.error('[connection_pool.js] Error occurs on wirte().');
      console.error(e);
      // MUST release self-assigned connection 
      // and kick the next queue
      clearRequest();
      throw e;
    }
    
  } // <-- function writeCommands() { ... } <--

  function clearRequest() {
    delete con.store, delete notReplied[requestId], cp.dequeue(rank);
    clearRequest = Function();
  } // <-- function clearRequest() { ... } <--

}

ConnectionPool.prototype.seekConnection = function(rank) {
  rank = rank || 0;
  var cp = this;
  var p = cp.pools, con = NULL, i;
  for( i = rank; i >= 0; i--) {
    if(con = p[i].seekAvailable()) return con;
  }
}

ConnectionPool.prototype.getAllConnections = function() {
  var r = [];
  this.forEachConnection(r.push.bind(r));
  return r;
//  return this.openConnections;
}

// Remove all non-needed event listeners
ConnectionPool.prototype.removeAllEventListeners = function() {
  this.removeAllListeners("close");
  this.removeAllListeners("error");
  this.removeAllListeners("timeout");
  this.removeAllListeners("connect");
  this.removeAllListeners("end");
  this.removeAllListeners("parseError");
  this.removeAllListeners("message");
  this.removeAllListeners("poolReady");
};

ConnectionPool.Event = {
  Ready: 'poolReady',
  Reconnect: 'poolReconnect'
};

ConnectionPool.State = {
  Disconnect: 'disconnect',
  Connecting: 'connecting',
  Connected: 'connected',
};

// ----------- //
function is(ty, x) {
  return typeof x == ty;
}
function isFunction(x) {
  return is('function', x);
}
function isArray(x) {
  return Array.isArray(x);
}

