var utils = require('./connection_utils'), inherits = require('util').inherits, net = require('net'), EventEmitter = require('events').EventEmitter, inherits = require('util').inherits, MongoReply = require("../responses/mongo_reply").MongoReply, Connection = require("./connection").Connection, Pool = require("./connection_pool_ranked").Pool;

//10-28-2013 [ystskm] debug function set >>
var util = require('util'), debug = process.env.NODE_DEBUG;
var DEBUG_CATEGORY = 'mongodb-native', DEBUG_KEY = 'db', FILE_NAME = __filename.split('/').slice(-1).toString();
debug = debug && (debug == 'true' || (new RegExp(DEBUG_CATEGORY + '|' + DEBUG_KEY).test(debug))) ?
  function(s) { util.error(FILE_NAME + ': ' + s) }: function() {};
//<< function debug() is available.

//4-4-2013 [ystskm] add
//singleton pools for exit
var ConnectionPools = [];

//4-4-2013 [ystskm] add
//callback target pool
var notReplied = {};

//4-4-2013 [ystskm] add
//Add close handler on process exit
process.on('exit', function() {
  ConnectionPools.forEach(function(cp) {
    cp.stop();
  });
});

//4-4-2013 [ystskm] add for debug
//var coid = 0;

//4-4-2013 [ystskm] add for debug
//var wrid = 0;

var ConnectionPool = exports.ConnectionPool = function(host, port, poolSize,
  bson, socketOptions) {
  if(typeof host !== 'string' || typeof port !== 'number')
    throw "host and port must be specified [" + host + ":" + port + "]";

  //[ystskm]
  var self = this;
  this.host = host, this.port = port;

  // Set up event emitter
  EventEmitter.call(this);
  // Keep all options for the socket in a specific collection allowing the user to specify the
  // Wished upon socket connection parameters
  this.socketOptions = typeof socketOptions === 'object' ? socketOptions: {};
  this.socketOptions.host = host;
  this.socketOptions.port = port;
  this.bson = bson;
  // PoolSize is always + 1 for special reserved "measurment" socket (like ping, stats etc)
  this.poolSize = poolSize;
  this.minPoolSize = Math.floor(this.poolSize / 2) + 1;

  // Set default settings for the socket options
  utils.setIntegerParameter(this.socketOptions, 'timeout', 0);
  // Delay before writing out the data to the server
  utils.setBooleanParameter(this.socketOptions, 'noDelay', true);
  // Delay before writing out the data to the server
  utils.setIntegerParameter(this.socketOptions, 'keepAlive', 0);
  // Set the encoding of the data read, default is binary == null
  utils.setStringParameter(this.socketOptions, 'encoding', null);
  // Allows you to set a throttling bufferSize if you need to stop overflows
  utils.setIntegerParameter(this.socketOptions, 'bufferSize', 0);

  // Internal structures
  this.openConnections = [];

  //[ystskm]
  this.pools = [].concat(poolSize).map(function(num, i) {
    return new Pool(self, i, parseInt(num) || 1);
  });

  //[ystskm] send waiting queues.
  this.queues = [];

  // Assign connection id's
  this.connectionId = 0;

  // Current index for selection of pool connection
  this.currentConnectionIndex = 0;
  // The pool state
  this._poolState = 'disconnected';
  // timeout control
  this._timeout = false;

  // [ystskm]
  ConnectionPools.push(this);

  // [ystskm]
  this.on('response', function(con) {
    var store = con.store;
    /* for debug 
    console.log('[connection_pool.js] return requestId: '
      + (store && store.requestId));
    */
    if(store)
      store.clearRequest();
    /* for debug
    console.log(Object.keys(notReplied));
    */
  });

}

inherits(ConnectionPool, EventEmitter);

ConnectionPool.prototype.setMaxBsonSize = function(maxBsonSize) {
  if(maxBsonSize == null) {
    maxBsonSize = Connection.DEFAULT_MAX_BSON_SIZE;
  }

  for( var i = 0; i < this.openConnections.length; i++) {
    this.openConnections[i].maxBsonSize = maxBsonSize;
  }
}

ConnectionPool.prototype.setMaxMessageSizeBytes = function(maxMessageSizeBytes) {
  if(maxMessageSizeBytes == null){
    maxMessageSizeBytes = Connection.DEFAULT_MAX_MESSAGE_SIZE;
  }

  for(var i = 0; i < this.openConnections.length; i++) {
    this.openConnections[i].maxMessageSizeBytes = maxMessageSizeBytes;
    this.openConnections[i].maxBsonSettings.maxMessageSizeBytes = maxMessageSizeBytes;
  }
}

// Start a function
var _connect = function(_self) {
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
    // [ystskm] >>
    var len = _self.pools.length;
    // set ready event for eack rank pool
    _self.pools.forEach(function(pool, rank) {
      // set ready / reconnect handler, go connecting
      pool.once('ready', onRankPoolReady);
      pool.on('reconnect', onRankPoolReconnect);
      pool.init();
      // rank pool ready
      function onRankPoolReady() {
        util.log('[connection_pool.js] pool ready for ' + _self.host + ':' + _self.port + ' (rank:' + pool.rank + ', size:' + pool.size + ')');
        if(--len == 0)
          _self._poolState = 'connected', _self.emit('poolReady');
      }
      function onRankPoolReconnect(n) {
        util.log('[connection_pool.js] pool reconnect for ' + _self.host + ':' + _self.port + ' (rank:' + pool.rank + ', size:' + pool.size + ', times:' + n + ')');
        if(--len == 0)
          _self._poolState = 'connected', _self.emit('poolReconnect');
      }
    });
    // <<
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
  if(typeof rank == 'function')
    fn = rank, rank = 0;
  var q = this.queues;
  if(!Array.isArray(q[rank]))
    q[rank] = [];
  q[rank].push(fn);
};

//[ystskm]
ConnectionPool.prototype.dequeue = function(rank) {
  for( var i = rank; i >= 0; i--) {
    var q = this.queues[rank];
    if(q && q.length)
      return q.shift()();
  }
};

// Start method, will throw error if no listeners are available
// Pass in an instance of the listener that contains the api for
// finding callbacks for a given message etc.
ConnectionPool.prototype.start = function() {
  var markerDate = new Date().getTime();
  var self = this;

  //[ystskm]
  if(this.isConnected())
    return;

  if(this.listeners("poolReady").length == 0) {
    throw "pool must have at least one listener ready that responds to the [poolReady] event";
  }

  // Set pool state to connecting
  this._poolState = 'connecting';
  this._timeout = false;

  _connect(self);
}

// Restart a connection pool (on a close the pool might be in a wrong state)
ConnectionPool.prototype.restart = function() {
  // Close all connections
  this.stop(false);
  // Now restart the pool
  this.start();
}

// Stop the connections in the pool
ConnectionPool.prototype.stop = function(removeListeners) {
  removeListeners = removeListeners == null ? true: removeListeners;
  // Set disconnected
  this._poolState = 'disconnected';

  // Clear all listeners if specified
  if(removeListeners) {
    this.removeAllEventListeners();
  }

  // Close all connections
  for( var i = 0; i < this.openConnections.length; i++) {
    this.openConnections[i].close();
  }

  // Clean up
  this.openConnections = [];

  // [ystskm]
  this.pools.forEach(function(pool) {
    pool.close();
  });
  // [ystskm]
  this.pools = [];

}

// Check the status of the connection
ConnectionPool.prototype.isConnected = function() {
  return this._poolState === 'connected';
}

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

// [ystskm]
ConnectionPool.prototype.write = function(commands, options, callback) {
  var self = this, con, requestId;
  options = options || {};

  var rank = options.rank || 0;
  try {

    requestId = [].concat(commands).pop().getRequestId();
    con = this.seekConnection(rank);

    if(!con) {
      /* for debug
      console.log(this.pools.map(function(pool, i){
        return i + ':' + (pool.pool.map(function(p){ return p.isFree() }).join(','));
      }).join(','));
      debug('stored.id:' + requestId);
      */
      return this.enqueue(rank, function() {
        self.write(commands, options, callback);
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

  } catch(e) {
    util.error('[connection_pool.js] Error occurs on wirte().');
    throw e;
  }

  debug(function() {
    var comm = Array.isArray(commands) ? commands[0]: commands;
    return 'reqId:' + requestId + ', conId:' + con.id + ', header:'
      + comm.collectionName + ', ' + JSON.stringify(comm.query);
  });

  con.write(commands, callback);

  if(commands.query || Array.isArray(commands) || options.async)
    return; // async event will emit from connection
  clearRequest();

  function clearRequest() {
    /* for debug 
    self.queues.forEach(function(a, rank) {
      if(a.length)
        console.log(a.length + ' queue at ' + self.poolSize + '.');
    });
    */
    delete con.store, delete notReplied[requestId], self.dequeue(rank);
    /* for debug
    var keys = Object.keys(notReplied);
    console.log('[connection_pool.js] rest ids: ' + Object.keys(notReplied));
    */
  }

}

ConnectionPool.prototype.seekConnection = function(rank) {
  rank = rank || 0;
  var con = null;
  for( var i = rank; i >= 0; i--)
    if(con = this.pools[i].seekAvailable())
      return con;
}

ConnectionPool.prototype.getAllConnections = function() {
  return this.openConnections;
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
}
