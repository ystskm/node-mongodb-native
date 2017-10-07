/***/
var NULL = null, TRUE = true, FALSE = false;
var Connection = require('./connection').Connection,
  ReadPreference = require('./read_preference').ReadPreference,
  DbCommand = require('../commands/db_command').DbCommand,
  MongoReply = require('../responses/mongo_reply').MongoReply,
  ConnectionPool = require('./connection_pool').ConnectionPool,
  EventEmitter = require('events').EventEmitter,
  ServerCapabilities = require('./server_capabilities').ServerCapabilities,
  Base = require('./base').Base,
  format = require('util').format,
  utils = require('../utils'),
  timers = require('timers'),
  inherits = require('util').inherits;

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

/**
 * Class representing a single MongoDB Server connection
 *
 * Options
 *  - **ssl** {Boolean, default:false}, use ssl connection (needs to have a mongod server with ssl support)
 *  - **sslValidate** {Boolean, default:false}, validate mongod server certificate against ca (needs to have a mongod server with ssl support, 2.4 or higher)
 *  - **sslCA** {Array, default:null}, Array of valid certificates either as Buffers or Strings (needs to have a mongod server with ssl support, 2.4 or higher)
 *  - **sslCert** {Buffer/String, default:null}, String or buffer containing the certificate we wish to present (needs to have a mongod server with ssl support, 2.4 or higher)
 *  - **sslKey** {Buffer/String, default:null}, String or buffer containing the certificate private key we wish to present (needs to have a mongod server with ssl support, 2.4 or higher)
 *  - **sslPass** {Buffer/String, default:null}, String or buffer containing the certificate password (needs to have a mongod server with ssl support, 2.4 or higher)
 *  - **poolSize** {Number, default:5}, number of connections in the connection pool, set to 5 as default for legacy reasons.
 *  - **socketOptions** {Object, default:null}, an object containing socket options to use (noDelay:(boolean), keepAlive:(number), connectTimeoutMS:(number), socketTimeoutMS:(number))
 *  - **logger** {Object, default:null}, an object representing a logger that you want to use, needs to support functions debug, log, error **({error:function(message, object) {}, log:function(message, object) {}, debug:function(message, object) {}})**.
 *  - **auto_reconnect** {Boolean, default:false}, reconnect on error.
 *  - **disableDriverBSONSizeCheck** {Boolean, default:false}, force the server to error if the BSON message is to big
 *
 * @class Represents a Server connection.
 * @param {String} host the server host
 * @param {Number} port the server port
 * @param {Object} [options] optional options for insert command
 */
function Server(host, port, options) {

  // Set up Server instance
  if(!(this instanceof Server)) return new Server(host, port, options);

  // Set up event emitter and callbackStore
  Base.call(this);

  // 10-28-2013 [ystskm] add for accept string port number.
  if(typeof port != 'number') {
    var to_port = parseInt(port) ? parseInt(port): Connection.DEFAULT_PORT;
    console.log('[mongodb-native:Server] Port is changed to:  "' + port + '(' + typeof port + ')" to "' + to_port + '(' + typeof to_port + ')"');
    port =  to_port;
  }
  // <--

  // Ensure correct values
  if(port != null && typeof port == 'object') {
    options = port;
    port = Connection.DEFAULT_PORT;
  }

  var self = this;
  this.host = host;
  this.port = port;
  this.options = options == null ? {} : options;
  this.internalConnection;
  this.internalMaster = false;
  this.connected = false;  
  this.poolSize = this.options.poolSize == null ? 5 : this.options.poolSize;
  this.disableDriverBSONSizeCheck = this.options.disableDriverBSONSizeCheck != null ? this.options.disableDriverBSONSizeCheck : false;
  this._used = false;
  this.replicasetInstance = null;

  // Emit open setup
  this.emitOpen = this.options.emitOpen || true;
  // Set ssl as connection method
  this.ssl = this.options.ssl == null ? false : this.options.ssl;
  // Set ssl validation
  this.sslValidate = this.options.sslValidate == null ? false : this.options.sslValidate;
  // Set the ssl certificate authority (array of Buffer/String keys)
  this.sslCA = Array.isArray(this.options.sslCA) ? this.options.sslCA : null;
  // Certificate to present to the server
  this.sslCert = this.options.sslCert;
  // Certificate private key if in separate file
  this.sslKey = this.options.sslKey;
  // Password to unlock private key
  this.sslPass = this.options.sslPass;
  // Server capabilities
  this.serverCapabilities = null;
  // Set server name
  this.name = format("%s:%s", host, port);

  // Ensure we are not trying to validate with no list of certificates
  if(this.sslValidate && (!Array.isArray(this.sslCA) || this.sslCA.length == 0)) {
    throw new Error("The driver expects an Array of CA certificates in the sslCA parameter when enabling sslValidate");
  }

  // Contains the isMaster information returned from the server
  this.isMasterDoc;

  // Set default connection pool options
  this.socketOptions = this.options.socketOptions != null ? this.options.socketOptions : {};
  if(this.disableDriverBSONSizeCheck) this.socketOptions.disableDriverBSONSizeCheck = this.disableDriverBSONSizeCheck;

  // Set ssl up if it's defined
  if(this.ssl) {
    this.socketOptions.ssl = true;
    // Set ssl validation
    this.socketOptions.sslValidate = this.sslValidate == null ? false : this.sslValidate;
    // Set the ssl certificate authority (array of Buffer/String keys)
    this.socketOptions.sslCA = Array.isArray(this.sslCA) ? this.sslCA : null;
    // Set certificate to present
    this.socketOptions.sslCert = this.sslCert;
    // Set certificate to present
    this.socketOptions.sslKey = this.sslKey;
    // Password to unlock private key
    this.socketOptions.sslPass = this.sslPass;
  }

  // Set up logger if any set
  this.logger = this.options.logger != null
    && (typeof this.options.logger.debug == 'function')
    && (typeof this.options.logger.error == 'function')
    && (typeof this.options.logger.log == 'function')
      ? this.options.logger : {error:function(message, object) {}, log:function(message, object) {}, debug:function(message, object) {}};

  // Just keeps list of events we allow
  this.eventHandlers = {error:[], parseError:[], poolReady:[], message:[], close:[], timeout:[]};
  // Internal state of server connection
  this._serverState = 'disconnected';
  // Contains state information about server connection
  this._state = {'runtimeStats': {'queryStats':new RunningStats()}};
  // Do we record server stats or not
  this.recordQueryStats = false;

  // Allow setting the socketTimeoutMS on all connections
  // to work around issues such as secondaries blocking due to compaction
  utils.setSocketTimeoutProperty(this, this.socketOptions);
};

/**
 * @ignore
 */
inherits(Server, Base);

//
//  Deprecated, USE ReadPreferences class
//
Server.READ_PRIMARY = ReadPreference.PRIMARY;
Server.READ_SECONDARY = ReadPreference.SECONDARY_PREFERRED;
Server.READ_SECONDARY_ONLY = ReadPreference.SECONDARY;

/**
 * Always ourselves
 * @ignore
 */
Server.prototype.setReadPreference = function(readPreference) {
  this._readPreference = readPreference;  
}

/**
 * @ignore
 */
Server.prototype.isMongos = function() {
  return this.isMasterDoc != null && this.isMasterDoc['msg'] == "isdbgrid" ? true : false;
}

/**
 * @ignore
 */
Server.prototype._isUsed = function() {
  return this._used;
}

/**
 * @ignore
 */
Server.prototype.close = function(callback) {
  
  // Set server status as disconnected
  this._serverState = 'destroyed';
  // Remove all local listeners
  this.removeAllListeners();

  if(this.connectionPool != null) {
    // Remove all the listeners on the pool so it does not fire messages all over the place
    this.connectionPool.removeAllEventListeners();
    // Close the connection if it's open
    this.connectionPool.stop(true);
  }

  // Emit close event
  var self = this;
  if(this.db && !this.isSetMember()) {
    processor(function() {
      self._emitAcrossAllDbInstances(self, null, "close", null, null, true)
    });
    // Flush out any remaining call handlers
    self._flushAllCallHandlers(utils.toError("Connection Closed By Application"));
  }

  // Peform callback if present
  if(typeof callback === 'function') callback(null);

};

Server.prototype.isDestroyed = function() {
  return this._serverState == 'destroyed';
}

/**
 * @ignore
 */
Server.prototype.isConnected = function() {
  return this.connectionPool != null && this.connectionPool.isConnected();
}

/**
 * @ignore
 */
Server.prototype.canWrite = Server.prototype.isConnected;
Server.prototype.canRead = Server.prototype.isConnected;

Server.prototype.isAutoReconnect = function() {
  if(this.isSetMember()) return false;
  return this.options.auto_reconnect != null ? this.options.auto_reconnect : true;
}

/**
 * @ignore
 */
Server.prototype.allServerInstances = function() {
  return [this];
}

/**
 * @ignore
 */
Server.prototype.isSetMember = function() {
  return this.replicasetInstance != null || this.mongosInstance != null;
}

/**
 * @ignore
 */
Server.prototype.setSocketOptions = function(options) {
  var connections = this.allRawConnections();
  for(var i = 0; i < connections.length; i++) {
    connections[i].setSocketOptions(options);
  }
}

/**
 * Assigns a replica set to this `server`.
 *
 * @param {ReplSet} replset
 * @ignore
 */
Server.prototype.assignReplicaSet = function (replset) {
  this.replicasetInstance = replset;
  this.inheritReplSetOptionsFrom(replset);
  this.enableRecordQueryStats(replset.recordQueryStats);
}

/**
 * Takes needed options from `replset` and overwrites
 * our own options.
 *
 * @param {ReplSet} replset
 * @ignore
 */
Server.prototype.inheritReplSetOptionsFrom = function (replset) {
  this.socketOptions = {};
  this.socketOptions.connectTimeoutMS = replset.options.socketOptions.connectTimeoutMS || 30000;

  if(replset.options.ssl) {
    // Set ssl on
    this.socketOptions.ssl = true;
    // Set ssl validation
    this.socketOptions.sslValidate = replset.options.sslValidate == null ? false : replset.options.sslValidate;
    // Set the ssl certificate authority (array of Buffer/String keys)
    this.socketOptions.sslCA = Array.isArray(replset.options.sslCA) ? replset.options.sslCA : null;
    // Set certificate to present
    this.socketOptions.sslCert = replset.options.sslCert;
    // Set certificate to present
    this.socketOptions.sslKey = replset.options.sslKey;
    // Password to unlock private key
    this.socketOptions.sslPass = replset.options.sslPass;
  }

  // If a socket option object exists clone it
  if(utils.isObject(replset.options.socketOptions)) {
    var keys = Object.keys(replset.options.socketOptions);
    for(var i = 0; i < keys.length; i++)
      this.socketOptions[keys[i]] = replset.options.socketOptions[keys[i]];
  }
}

/**
 * Opens this server connection.
 *
 * @ignore
 */
Server.prototype.connect = function(dbInstance, options, callback) {

  if(isFunction(options)) callback = options, options = {};
  if(options == NULL) options = {};
  if(!isFunction(callback)) callback = NULL;
  
  var server = this, self = server;
  // Save the options
  server.options = options;

  // Currently needed to work around problems with multiple connections in a pool with ssl
  // TODO fix if possible
  if(this.ssl == TRUE) {
    // Set up socket options for ssl
    this.socketOptions.ssl = true;
    // Set ssl validation
    this.socketOptions.sslValidate = this.sslValidate == null ? false : this.sslValidate;
    // Set the ssl certificate authority (array of Buffer/String keys)
    this.socketOptions.sslCA = Array.isArray(this.sslCA) ? this.sslCA : null;
    // Set certificate to present
    this.socketOptions.sslCert = this.sslCert;
    // Set certificate to present
    this.socketOptions.sslKey = this.sslKey;
    // Password to unlock private key
    this.socketOptions.sslPass = this.sslPass;
  }

  // Let's connect
  // Let's us override the main receiver of events
  var eventReceiver = options.eventReceiver != NULL ? options.eventReceiver : server;

  // Save reference to dbInstance
  server.db = dbInstance;  // `db` property matches ReplSet and Mongos
  server.dbInstances = [dbInstance];
  
  // 6-4-2013 [ystskm] Not to reconnect on pool reconnecting -->
  var cp = server.connectionPool;
  var alreadyConnected = function() {
    
    var internalCallback = callback;
    callback = null;
    if(isFunction(internalCallback)) internalCallback(null, dbInstance, server);
    
  };
  if(cp) {
    return cp.isConnected() ? alreadyConnected(): cp.once('poolReady', alreadyConnected);
  }
  // <--

  // Force connection pool if there is one
  // if(server.connectionPool) server.connectionPool.stop();
  
  // Set server state to connecting
  server._serverState = 'connecting';

  /*
  if(server.connectionPool != null) {
    // Remove all the listeners on the pool so it does not fire messages all over the place
    this.connectionPool.removeAllEventListeners();
    // Close the connection if it's open
    this.connectionPool.stop(true);    
  } */

  server.connectionPool = new ConnectionPool(server.host, server.port, server.poolSize, dbInstance.bson, this.socketOptions);
  var connectionPool = cp = server.connectionPool;
  
  // If ssl is not enabled don't wait between the pool connections
  if(this.ssl == null || !this.ssl) {
    connectionPool._timeToWait = null;
  }

  // Set logger on pool
  connectionPool.logger = server.logger;
  connectionPool.bson = dbInstance.bson;

  // Set basic parameters passed in
  var returnIsMasterResults = options.returnIsMasterResults == NULL ? FALSE : options.returnIsMasterResults;

  // Create a default connect handler, overriden when using replicasets
  var connectCallback = function() {
    return function(err, reply) {
      
      // ensure no callbacks get called twice
      var internalCallback = callback;
      callback = NULL;
      
      // If something close down the connection and removed the callback before
      // proxy killed connection etc, ignore the error as close event was issued

      // Internal callback
      if(err != NULL) {
        if(isFunction(internalCallback)) internalCallback(err, NULL, server);
        return;
      }
      
      if(reply == NULL || reply.documents == NULL) {
        return internalCallback(utils.toError("server failed to respond", NULL, server));
      }
      
      server.master = reply.documents[0].ismaster == 1 ? TRUE : FALSE;
      server.connectionPool.setMaxBsonSize(reply.documents[0].maxBsonObjectSize);
      server.connectionPool.setMaxMessageSizeBytes(reply.documents[0].maxMessageSizeBytes);
      server.connectionPool.setMaxWriteBatchSize(reply.documents[0].maxWriteBatchSize);
      
      // Set server state to connEcted
      server._serverState = 'connected';
      // Set server as connected
      server.connected = TRUE;
      // Save document returned so we can query it
      server.isMasterDoc = reply.documents[0];
      
      if(server.emitOpen) {        
        server._emitAcrossAllDbInstances(server, eventReceiver, "open", NULL, returnIsMasterResults ? reply : NULL, NULL);        
        server.emitOpen = FALSE;
      } else {
        server._emitAcrossAllDbInstances(server, eventReceiver, "reconnect", NULL, returnIsMasterResults ? reply : NULL, NULL);        
      }

      // Set server capabilities
      server.serverCapabilities = new ServerCapabilities(server.isMasterDoc);      

      // Set server capabilities on all the connections
      var i, connections = connectionPool.getAllConnections();
      for(i = 0; i < connections.length; i++) {
        connections[i].serverCapabilities = server.serverCapabilities;
      }

      // 07-03-2015 ystskm
      // Inherits for ranked pool
      server.connectionPool.serverCapabilities = server.serverCapabilities;
      // <--
      
      // If we have it set to returnIsMasterResults
      if(!isFunction(internalCallback)) {
        return;
      }
      if(returnIsMasterResults) {
        internalCallback(NULL, reply, server);
      } else {
        internalCallback(NULL, dbInstance, server);
      }

    };
  }; // <-- var connectCallback = function() { ... } <--

  // Let's us override the main connect callback
  var connectHandler = options.connectHandler == NULL ? connectCallback() : options.connectHandler;

  // Set up on connect, message, timeout, error, parseError and close
  connectionPool.on("poolReady", onPoolReady);
  connectionPool.on("poolReconnect", onPoolReady);
  connectionPool.on("message", onPoolMessage);
  connectionPool.on("timeout", onPoolTimeout);
  connectionPool.on("error", onPoolError);
  connectionPool.on("parseError", onPoolParseError);
  connectionPool.on("close", onPoolClose);
  
  // Boot up connection poole, pass in a locator of callbacks
  connectionPool.start();
    
  function onPoolReady(n) {
    
    debug('Detect pool ready:', n);
    
    // Create db command and Add the callback to the list of callbacks by the request id (mapping outgoing messages to correct callbacks)
    var db_command = DbCommand.NcreateIsMasterCommand(dbInstance, dbInstance.databaseName);
    
    // Check out a reader from the pool
    // 10-28-2013 [ystskm] modify for rank service
    // var connection = connectionPool.checkoutConnection();
    var connection = connectionPool.checkoutRankConnection();
    // <--
    
    // Register handler for messages
    server._registerHandler(db_command, FALSE, connection, connectHandler);
    
    // 10-05-2017 [ystskm] Not to fire twice
    connectHandler = function() {
      outLog('Nth callback for NcreateIsMasterCommand on pool ready.');
    };

    // Write the command out
    connection.write(db_command);

  }

  // Set up item connection
  function onPoolMessage(message, connection) {
    // Attempt to parse the message
    var cp = this;
    try {

      // [ystskm] special direct reply pattern 
      // ( for connection level timeout, etc ... )
      if(message instanceof Error) {
        server._callHandler(arguments[1], null, arguments[0]);
        return;
      }

      // Create a new mongo reply
      var mongoReply = new MongoReply()
      // Parse the header
      mongoReply.parseHeader(message, connectionPool.bson)

      // If message size is not the same as the buffer size
      // something went terribly wrong somewhere
      if(mongoReply.messageLength != message.length) {
        // Emit the error
        if(eventReceiver.listeners("error") && eventReceiver.listeners("error").length > 0) eventReceiver.emit("error", new Error("bson length is different from message length"), server);
        // Remove all listeners
        server.removeAllListeners();
        return;
      }

      // Callback instance
      var callbackInfo = server._findHandler(mongoReply.responseTo.toString());
      // Abort if not a valid callbackInfo, don't try to call it
      if(callbackInfo == NULL || callbackInfo.info == NULL) {
        outLog('Abort if not a valid callbackInfo, don\'t try to call it')
        return;
      }

      // The command executed another request, log the handler again under that request id
      if(mongoReply.requestId > 0 && mongoReply.cursorId.toString() != "0" 
        && callbackInfo && callbackInfo.info && callbackInfo.info.exhaust) {
          server._reRegisterHandler(mongoReply.requestId, callbackInfo);
      }

      // Parse the body
      mongoReply.parseBody(message, connectionPool.bson, callbackInfo.info.raw, function(err) {
        
        var stamp = new Date();
        if(err != NULL) {
          outLog('Unexpected parseBody state error.', err);
        }
        /*
        if(err != null) {
          
          outLog('Unexpected parseBody state error! Server will be disconnected.', err);
          
          // If pool connection is already closed
          if(server._serverState === 'disconnected') return;
          // Set server state to disconnected
          server._serverState = 'disconnected';

          // Remove all listeners and close the connection pool
          server.removeAllListeners();
          connectionPool.stop(true);

          // If we have a callback return the error
          if(isFunction(callback)) {
            
            // ensure no callbacks get called twice
            var internalCallback = callback; 
            callback = null;
            // Perform callback
            if(isFunction(internalCallback)) internalCallback(err, NULL, server);
            
          } else if(server.isSetMember()) {
            if(server.listeners("parseError") && server.listeners("parseError").length > 0) server.emit("parseError", utils.toError(err), server);
          } else {
            if(eventReceiver.listeners("parseError") && eventReceiver.listeners("parseError").length > 0) eventReceiver.emit("parseError", utils.toError(err), server);
          }

          // If we are a single server connection fire errors correctly
          if(!server.isSetMember()) {
            // Fire all callback errors
            server.__executeAllCallbacksWithError(err);
            // Emit error
            server._emitAcrossAllDbInstances(server, eventReceiver, "parseError", server, null, true);
          }
          // Short cut
          return;
        } */

        // Let's record the stats info if it's enabled
        if(server.recordQueryStats == TRUE && server._state['runtimeStats'] != NULL
          && server._state.runtimeStats['queryStats'] instanceof RunningStats) {
          // Add data point to the running statistics object
          server._state.runtimeStats.queryStats.push(stamp.getTime() - callbackInfo.info.start);
        }

        // [2017.10.03 sakamoto] add reconnection
        var document = (mongoReply.documents || [])[0] || {};
        var fatal;
        
        // NON "document.code" ERROR TREAT AS CRITICAL
        // DO NOT TREAT AS CRITICAL for simple-query error.
        if(err && !document.code) {
          fatal = [err.name, err.note, err.errmsg].join(' ');
        }
        
        // If we have an error about the server "could not contact primary for replica set: rsn"
        // code: 7 or 8
        if(document.code == 7 || document.code == 8) {
          fatal = 'MongodbMonitor could not contact primary for replica set';
        }
        
        // write results unavailable from 10.0.101.187:27018 :: caused by :: Location17255 error receiving write command response, possible socket exception -
        // code: 83
        if(document.code == 83) {
          fatal = 'Mongodb error by possible socket exception';
        }
        
        // exception: DBClientBase::findN: transport error: 10.0.x.x ns: 
        // code: 10276
        if(document.code == 10276) {
          fatal = 'Mongodb error by transport error';
        }
        
        // If we have an error about the server "exception: socket exception [CONNECT_ERROR] for rs1/10.0.x.x:00000,10.0.x.x:00000"
        // code: 11002
        if(document.code == 11002) {
          fatal = 'Mongodb error by socket exception';
        }
        
        // If we have an error about the server "ReplicaSetMonitor no master found for set: rsn"
        // code: 10009
        if(document.code == 10009) {
          fatal = 'ReplicaSetMonitor no master found for set';
        }
                
        // If we have an error about the server "not being master or primary"
        // code: 13436
        if((mongoReply.responseFlag & (1 << 1)) != 0
          && document.code == 13436) {
          fatal = 'MongodbMonitor not being master';
        }
        
        // If fatal error, connection must be reconnect synchronously
        // not to write to the discard connections in the server._callHandler callback
        if(fatal) {
          console.log(stamp.toGMTString() + ' - [Mongodb] Detect ' + fatal);
          console.log('  at connection:#' + connection.id, ', replyId:' + mongoReply.responseTo + ', reply:', document);
          cp.isConnected() && cp.restart(); // => refresh database connections
        }
        
        // Dispatch the call
        server._callHandler(mongoReply.responseTo, mongoReply, err);

        // Flush all non-reply listeners
        if(fatal) {
          server._flushAllCallHandlers(err, document);
        }
        
      });
      
    } catch (err) {
      // Throw error in next tick
      processor(function() {
        throw err;
      });
    }
  }

  // Handle timeout
  function onPoolTimeout(err) {
    // If pool connection is already closed
    if(server._serverState === 'disconnected' 
      || server._serverState === 'destroyed') return;
    // Set server state to disconnected
    server._serverState = 'disconnected';
    // If we have a callback return the error
    if(typeof callback === 'function') {
      
      // ensure no callbacks get called twice
      var internalCallback = callback; 
      callback = null;
      // Perform callback
      if(isFunction(internalCallback)) internalCallback(err, null, server);
      
    } else if(server.isSetMember()) {
      if(server.listeners("timeout") && server.listeners("timeout").length > 0) server.emit("timeout", err, server);
    } else {
      if(eventReceiver.listeners("timeout") && eventReceiver.listeners("timeout").length > 0) eventReceiver.emit("timeout", err, server);
    }

    // If we are a single server connection fire errors correctly
    if(!server.isSetMember()) {
      // Fire all callback errors
      server.__executeAllCallbacksWithError(err);
      // Emit error
      server._emitAcrossAllDbInstances(server, eventReceiver, "timeout", err, server, true);
    }

    // If we have autoConnect enabled let's fire up an attempt to reconnect
    if(server.isAutoReconnect() 
      && !server.isSetMember()
      && (server._serverState != 'destroyed')
      && !server._reconnectInProgreess) {
      // Set the number of retries
      server._reconnect_retries = server.db.numberOfRetries;
      // Attempt reconnect
      server._reconnectInProgreess = true;
      setTimeout(__attemptReconnect(server), server.db.retryMiliSeconds);
    }    
  }

  // Handle errors
  function onPoolError(message, connection, error_options) {
    // If pool connection is already closed
    if(server._serverState === 'disconnected' 
      || server._serverState === 'destroyed') return;
    
    // Set server state to disconnected
    server._serverState = 'disconnected';
    // Error message
    var error_message = new Error(message && message.err ? message.err : message);
    // Error message coming from ssl
    if(error_options && error_options.ssl) error_message.ssl = true;

    // If we have a callback return the error
    if(typeof callback === 'function') {
      
      // ensure no callbacks get called twice
      var internalCallback = callback; 
      callback = null;
      // Perform callback
      if(isFunction(internalCallback)) internalCallback(error_message, null, server);
      
    } else if(server.isSetMember()) {
      if(server.listeners("error") && server.listeners("error").length > 0) server.emit("error", error_message, server);
    } else {
      if(eventReceiver.listeners("error") && eventReceiver.listeners("error").length > 0) eventReceiver.emit("error", error_message, server);
    }

    // If we are a single server connection fire errors correctly
    if(!server.isSetMember()) {
      // Fire all callback errors
      server.__executeAllCallbacksWithError(error_message);
      // Emit error
      server._emitAcrossAllDbInstances(server, eventReceiver, "error", error_message, server, true);
    }

    // If we have autoConnect enabled let's fire up an attempt to reconnect
    if(server.isAutoReconnect() 
      && !server.isSetMember()
      && (server._serverState != 'destroyed')
      && !server._reconnectInProgreess) {

      // Set the number of retries
      server._reconnect_retries = server.db.numberOfRetries;
      // Attempt reconnect
      server._reconnectInProgreess = true;
      setTimeout(__attemptReconnect(server), server.db.retryMiliSeconds);
    }    
  }

  // Handle close events
  function onPoolClose() {
    // If pool connection is already closed
    if(server._serverState === 'disconnected' 
      || server._serverState === 'destroyed') return;
    // Set server state to disconnected
    server._serverState = 'disconnected';
    // If we have a callback return the error
    if(typeof callback == 'function') {
      
      // ensure no callbacks get called twice
      var internalCallback = callback;
      callback = null;
      // Perform callback
      if(isFunction(internalCallback)) internalCallback(new Error("connection closed"), null, server);
      
    } else if(server.isSetMember()) {
      if(server.listeners("close") && server.listeners("close").length > 0) server.emit("close", new Error("connection closed"), server);
    } else {
      if(eventReceiver.listeners("close") && eventReceiver.listeners("close").length > 0) eventReceiver.emit("close", new Error("connection closed"), server);
    }

    // If we are a single server connection fire errors correctly
    if(!server.isSetMember()) {
      // Fire all callback errors
      server.__executeAllCallbacksWithError(new Error("connection closed"));
      // Emit error
      server._emitAcrossAllDbInstances(server, eventReceiver, "close", server, null, true);
    }

    // If we have autoConnect enabled let's fire up an attempt to reconnect
    if(server.isAutoReconnect() 
      && !server.isSetMember()
      && (server._serverState != 'destroyed')
      && !server._reconnectInProgreess) {

      // Set the number of retries
      server._reconnect_retries = server.db.numberOfRetries;  
      // Attempt reconnect
      server._reconnectInProgreess = true;
      setTimeout(__attemptReconnect(server), server.db.retryMiliSeconds);
    }    
  }

  /**
   * @ignore
   */
  var __attemptReconnect = function(server) {
    return function() {
      // Attempt reconnect
      server.connect(server.db, server.options, function(err, result) {
        server._reconnect_retries = server._reconnect_retries - 1;

        if(err) {
          // Retry
          if(server._reconnect_retries == 0 || server._serverState == 'destroyed') {
            server._serverState = 'connected';
            server._reconnectInProgreess = false
            // Fire all callback errors
            return server.__executeAllCallbacksWithError(new Error("failed to reconnect to server"));
          } else {
            return setTimeout(__attemptReconnect(server), server.db.retryMiliSeconds);
          }
        } else {
          // Set as authenticating (isConnected will be false)
          server._serverState = 'authenticating';
          // Apply any auths, we don't try to catch any errors here
          // as there are nowhere to simply propagate them to
          self._apply_auths(server.db, function(err, result) {            
            server._serverState = 'connected';
            server._reconnectInProgreess = false;

            // Execute any buffered reads and writes
            server._commandsStore.execute_queries();
            server._commandsStore.execute_writes();
            // Emit reconnect event
            server.emit('reconnect');
          });
        } 
      });      
    }
  }

  // If we have a parser error we are in an unknown state, close everything and emit
  // error
  function onPoolParseError(err, requestId) {
    // [ystskm] don't close with "parseError"
//    // If pool connection is already closed
//    if(server._serverState === 'disconnected' 
//      || server._serverState === 'destroyed') return;
//    // Set server state to disconnected
//    server._serverState = 'disconnected';
    // If we have a callback return the error
    if(typeof callback === 'function') {
      
      // ensure no callbacks get called twice
      var internalCallback = callback;
      callback = null;
      // Perform callback
      if(isFunction(internalCallback)) internalCallback(utils.toError(err), null, server);
      
    } else if(server.isSetMember()) {
      if(server.listeners("parseError") && server.listeners("parseError").length > 0) server.emit("parseError", utils.toError(err), server);
    } else {
      if(eventReceiver.listeners("parseError") && eventReceiver.listeners("parseError").length > 0) eventReceiver.emit("parseError", utils.toError(err), server);
    }
    
//    // If we are a single server connection fire errors correctly
//    if(!server.isSetMember()) {
//      // Fire all callback errors
//      server.__executeAllCallbacksWithError(utils.toError(err));
//      // Emit error
//      server._emitAcrossAllDbInstances(server, eventReceiver, "parseError", server, null, true);
//      // Emit close event
//      server._emitAcrossAllDbInstances(server, eventReceiver, "close", new Error("connection closed"), null, true);
//    }
    
    // Dispatch the call
    // when "err != null and result == null", then an error callback fires!
    server._callHandler(requestId, null, err);
    
  }
  
}

/**
 * @ignore
 */
Server.prototype.allRawConnections = function() {
  return this.connectionPool != null ? this.connectionPool.getAllConnections() : [];
}

/**
 * Check if a writer can be provided
 * @ignore
 */
var canCheckoutWriter = function(self, read) {
  var error = null;

  // We cannot write to an arbiter or secondary server
  if(self.isMasterDoc && self.isMasterDoc['arbiterOnly'] == true) {
    error = new Error("Cannot write to an arbiter");
    error.code = -5000;
  } if(self.isMasterDoc && self.isMasterDoc['secondary'] == true) {
    error = new Error("Cannot write to a secondary");
    error.code = -5000;
  } else if(read == true && self._readPreference == ReadPreference.SECONDARY && self.isMasterDoc && self.isMasterDoc['ismaster'] == true) {
    error = new Error("Cannot read from primary when secondary only specified");
    error.code = -5000;
  } else if(!self.isMasterDoc) {
    error = new Error("Cannot determine state of server");
    error.code = -5000;
  }

  // Return no error
  return error;
}

/**
 * @ignore
 */
Server.prototype.checkoutWriter = function(read) {
  if(this._serverState == 'disconnected' || this._serverState == 'destroyed')
    return null;
  // 10-28-2013 [ystskm] modify for rank service
  // if(read == true) return this.connectionPool.checkoutConnection();
  if(read == true) return this.connectionPool.checkoutRankConnection();
  // <--
  // Check if are allowed to do a checkout (if we try to use an arbiter f.ex)
  var result = canCheckoutWriter(this, read);
  // If the result is null check out a writer
  if(result == null && this.connectionPool != null) {
    // 10-28-2013 [ystskm] modify for rank service 
    // 06-03-2015 [ystskm] fix for v1.4.33
    // return this.connectionPool.checkoutConnection();
    return this.connectionPool.checkoutRankConnection();
    // <--
  }
  if(result == null) {
    return null;
  } else {
    return result;
  }
}

/**
 * Check if a reader can be provided
 * @ignore
 */
var canCheckoutReader = function(self) {
  var error = null;
  
  // We cannot write to an arbiter or secondary server
  if(self.isMasterDoc && self.isMasterDoc['arbiterOnly'] == true && self.isSetMember()) {
    error = new Error("Cannot write to an arbiter");
  } else if(self._readPreference != null) {
    // If the read preference is Primary and the instance is not a master return an error
    if((self._readPreference == ReadPreference.PRIMARY) && self.isMasterDoc && self.isMasterDoc['ismaster'] != true) {
      error = new Error("Read preference is Server.PRIMARY and server is not master");
    } else if(self._readPreference == ReadPreference.SECONDARY && self.isMasterDoc && self.isMasterDoc['ismaster'] == true) {
      error = new Error("Cannot read from primary when secondary only specified");
    }
  } else if(!self.isMasterDoc) {
    error = new Error("Cannot determine state of server");
  }

  // Return no error
  return error;
}

/**
 * @ignore
 */
Server.prototype.checkoutReader = function(read) {
  if(this._serverState == 'disconnected' || this._serverState == 'destroyed')
    return null;
  // Check if are allowed to do a checkout (if we try to use an arbiter f.ex)
  var result = canCheckoutReader(this);
  // If the result is null check out a writer
  if(result == null && this.connectionPool != null) {
    // 10-28-2013 [ystskm] modify for rank service
    // 06-03-2015 [ystskm] fix for v1.4.33 -->
    // var connection = this.connectionPool.checkoutConnection();
    return this.connectionPool.checkoutRankConnection();
    // <--
  } else if(result == null) {
    return null;
  } else {
    return result;
  }
}

/**
 * @ignore
 */
Server.prototype.enableRecordQueryStats = function(enable) {
  this.recordQueryStats = enable;
}

/**
 * Internal statistics object used for calculating average and standard devitation on
 * running queries
 * @ignore
 */
var RunningStats = function() {
  var self = this;
  this.m_n = 0;
  this.m_oldM = 0.0;
  this.m_oldS = 0.0;
  this.m_newM = 0.0;
  this.m_newS = 0.0;

  // Define getters
  Object.defineProperty(this, "numDataValues", { enumerable: true
    , get: function () { return this.m_n; }
  });

  Object.defineProperty(this, "mean", { enumerable: true
    , get: function () { return (this.m_n > 0) ? this.m_newM : 0.0; }
  });

  Object.defineProperty(this, "variance", { enumerable: true
    , get: function () { return ((this.m_n > 1) ? this.m_newS/(this.m_n - 1) : 0.0); }
  });

  Object.defineProperty(this, "standardDeviation", { enumerable: true
    , get: function () { return Math.sqrt(this.variance); }
  });

  Object.defineProperty(this, "sScore", { enumerable: true
    , get: function () {
      var bottom = this.mean + this.standardDeviation;
      if(bottom == 0) return 0;
      return ((2 * this.mean * this.standardDeviation)/(bottom));
    }
  });
}

/**
 * @ignore
 */
RunningStats.prototype.push = function(x) {
  // Update the number of samples
  this.m_n = this.m_n + 1;
  
  // See Knuth TAOCP vol 2, 3rd edition, page 232
  if(this.m_n == 1) {
    this.m_oldM = this.m_newM = x;
    this.m_oldS = 0.0;
  } else {
    this.m_newM = this.m_oldM + (x - this.m_oldM) / this.m_n;
    this.m_newS = this.m_oldS + (x - this.m_oldM) * (x - this.m_newM);
    // set up for next iteration
    this.m_oldM = this.m_newM;
    this.m_oldS = this.m_newS;
  }
}

/**
 * @ignore
 */
Object.defineProperty(Server.prototype, "autoReconnect", { enumerable: true
  , get: function () {
      return this.options['auto_reconnect'] == null ? false : this.options['auto_reconnect'];
    }
});

/**
 * @ignore
 */
Object.defineProperty(Server.prototype, "connection", { enumerable: true
  , get: function () {
      return this.internalConnection;
    }
  , set: function(connection) {
      this.internalConnection = connection;
    }
});

/**
 * @ignore
 */
Object.defineProperty(Server.prototype, "master", { enumerable: true
  , get: function () {
      return this.internalMaster;
    }
  , set: function(value) {
      this.internalMaster = value;
    }
});

/**
 * @ignore
 */
Object.defineProperty(Server.prototype, "primary", { enumerable: true
  , get: function () {
      return this;
    }
});

/**
 * Getter for query Stats
 * @ignore
 */
Object.defineProperty(Server.prototype, "queryStats", { enumerable: true
  , get: function () {
      return this._state.runtimeStats.queryStats;
    }
});

/**
 * @ignore
 */
Object.defineProperty(Server.prototype, "runtimeStats", { enumerable: true
  , get: function () {
      return this._state.runtimeStats;
    }
});

/**
 * Get Read Preference method
 * @ignore
 */
Object.defineProperty(Server.prototype, "readPreference", { enumerable: true
  , get: function () {
      if(this._readPreference == null && this.readSecondary) {
        return Server.READ_SECONDARY;
      } else if(this._readPreference == null && !this.readSecondary) {
        return Server.READ_PRIMARY;
      } else {
        return this._readPreference;
      }
    }
});

/**
 * @ignore
 */
exports.Server = Server;

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
