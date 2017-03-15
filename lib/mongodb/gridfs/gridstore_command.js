/***/
var NULL = null, TRUE = true, FALSE = false;
var fs = require('fs'), util = require('util');
var Emitter = require('events').EventEmitter, Chunk = require('./chunk').Chunk;
var timer = require('../timer');

var PREVIOUS_HANDLED;
if(!global.Promise)
  global.Promise = require('es6-promise');

// file status
var FileIs = {
  STABLE: 0,
  BEFORE_STABLE: 1,
  ON_UPDATE: 2,
  BEFORE_DELETE: 3
};

// emitted by _reader
var DataEvent = {
  Data: 'data',
  Error: 'error',
  End: 'end'
};

var Err = {

  EPERM: errCodeGen('EPERM', 1, 'Operation not permitted'),
  ENOENT: errCodeGen('ENOENT', 34, 'No such file'),
  EBADF: errCodeGen('EBADF', 9, 'Bad filename'),
  EEXIST: errCodeGen('EEXIST', 17, 'File already exists'),

  colOpen: errFnGen('Collection open failed.', 'Collection.open'),
  colFind: errFnGen('Collection find failed.', 'Collection.find'),
  colUpdate: errFnGen('Collection update failed.', 'Collection.update')

};

//mode status
var OpenModeIs = {
  read: function(mode) {
    return mode == 'r';
  },
  write: function(mode) {
    return mode == 'w' || mode == 'w+';
  },
  overwrite: function(mode) {
    return mode == 'w';
  }
};

var Collections = {
  files: '.files',
  chunks: '.chunks',
  stats: '.conditions'
};

var CollectionsList = Object.keys(Collections).map(function(name) {
  return name + 'Collection';
});

/**
 * GridStoreCommands
 */
module.exports = Commands;

function Commands(gridstore) {

  var gc = this;

  // inherits from "exports.GridStore"
  var gs = gc.gs = gridstore;

  gc.db = gs.db;
  gc.ObjectID = gc.db.bson_serializer.ObjectID

  gc.root = gs.root;
  gc.referenceBy = gs.referenceBy;

  gc.filesId = gs.filesId;
  gc.filename = gs.filename;

  gc.defaultChunkSize = gs.defaultChunkSize;
  gc.contentType = gs.contentType;
  gc.mode = gs.mode;
  gc.safe = gs.safe;

  // the most important data(number) for NON_BLOCKING IO
  gc._no = null, gc._files = null;

  // internal position for chunk reading / writing
  gc.internalPosition = 0;

  // self event emitter chunk writing
  gc.incomingDataLength = 0;

  // initialize condition
  // TODO after open is better?
  gc.rewind();

};

/**
 * 
 */
Commands.prototype.seek = function(pos) {
  this.internalPosition = pos;
};

Commands.prototype.errHandler = function(msg, err, handle) {

  var gc = this;
  var stack;

  if(isFunction( err ) || isErrorObject( err )) {
    handle = err, err = NULL;
  }
  
  // When a simple error thrown.
  if(isErrorObject( msg )) {
    err = msg, msg = '[gridstore_command] errHandler: ';
    stack = msg.stack;
  } else {
    err = err || {};
    stack = NULL;
  }

  // Direct returns filter
  if(err.message == Err.ENOENT) {
    return handle(err);
  }

  // additional information for debug.
  var filemsg, timemsg = new Date().toGMTString();
  if(gc.filename) {
    filemsg = '(filename: ' + gc.filename + ')';
  } else {
    filemsg = '(unknown filename)';
  }
  if(PREVIOUS_HANDLED === gc) {

    // Recursive error in some condition?
    try { gc.close(Function()); } catch(e) {  };
    return console.log(timemsg + ' - Recursive error message?', 
      msg, err, stack, handle);

  }
  PREVIOUS_HANDLED = gc;
  msg = (msg ? msg + "\n": "") + filemsg;

  // Output error message.
  // TODO Ignore "ENOENT" and so on.
  console.log(timemsg + ' - ' + msg);
  console.log('  ORIGINAL ERROR:', err, stack, handle);
  err = new Error(msg);

  var afterClose = Function();
  switch(TRUE) {
  
  case isFunction( handle ):
    afterClose = function(e) {
      handle(err);
    };
    break;
    
  case isErrorObject( handle ):
    afterClose = function(e) {
      handle.emit('error', err);
    };
    break;
    
  }

  // release lock
  gc.close(afterClose);

};

Commands.prototype.open = function(callback) {

  var gc = this;
  var gs = gc.gs, gsid = gs.gsid;
  //  console.log('OPEN (' + gsid + ') ' + gc.filename);

  var mode = gc.mode, f_col;
  var openCallback = function(er) {
    // try { ... } catch(e) { ... } to avoid the unexpected unlocked finish
    try {
      if(er) throw er; callback(null, gc);
    } catch(e) {
      gc.errHandler(e, callback);
    }
  };

  // already open or not
  gc._no == null ? getLatest(): setImmediate(openCallback);
  return;

  function getLatest() {
    if(!gc.filename)
      return openCallback(new Error(Err.EBADF));
    gc.filesCollection(function(er, col) {
      er ? openCallback(er): (f_col = col).findOne({

        filename: gc.filename,
        _writing: FileIs.STABLE

      }, {

        sort: {
          uploadDate: -1
        }

      }, registerPresent);
    });
  }

  function registerPresent(er, obj) {

    if(er)
      return openCallback(er);

    if(obj == null && OpenModeIs.read(mode))
      return openCallback(new Error(Err.ENOENT));

    // the metadata provider
    gc._files = obj = obj || {};

    // the file identifier, like file descriptor for each file.
    gc._no = obj._no || 0;

    OutDebug(5, 'open', function() {
      console.log('GETS FOR OPEN (PRESENT).');
      console.log(gc.filename, gc._no, obj);
    });

    // build new files data when write.
    if(OpenModeIs.write(mode)) {

      delete obj._id, delete obj._no;
      delete gc._no;

      gc.setMongoObjectToSelf(obj);
      gc.buildMongoObject(FileIs.STABLE, getNext);
      return;

    }

    // "metadata.atime" will be set when read
    // to avoid "deleteChunks" by a write process.
    f_col.findAndModify({
      _id: obj._id
    }, [], {
      $set: {
        'metadata.atime': obj['metadata'].atime = new Date()
      }
    }, {
      upsert: false
    }, function(er) {
      if(er)
        return openCallback(er);
      gc.setMongoObjectToSelf(obj);
      gc.buildMongoObject(FileIs.STABLE, openCallback);
    });

  }

  function getNext(er) {
    er ? openCallback(er): gc.statsCollection(function(er, s_col) {
      er ? openCallback(er): s_col.findAndModify({

        // query
        _id: gc.filename

      }, [], { // sort | update document

        $inc: {
          _no: 1
        }

      }, { // options

        upsert: true

      }, registerNext);
    });
  }

  function registerNext(er, obj) {

    if(er)
      return openCallback(er);

    gc._no = ((obj || '')._no || 0) + 1;

    OutDebug(5, 'open', function() {
      console.log('GOOD END FOR "WRITE" OPEN.');
      console.log(gc.filename, gc._no, gc._stat);
    });

    if(OpenModeIs.overwrite(mode)) {
      return gc.rewind(), openCallback();
    }

    // copy present chunks to "append writing"
    openCallback(new Error('Now, w+ is not supported yet.'));

  }

};

// >> ------------ _reader : chunks reading class ------------ >>
/**
 * @constructor _reader
 */
function _reader(gc, options) {
  var _r = this;

  Emitter.call(_r);

  _r.gc = gc;
  otpions = options || {}

  var size = parseInt(options.size || null), offset = options.offset || 0;
  var chunk_size = options.chunkSize || gc.internalChunkSize
    || gc.defaultChunkSize;

  // offset => _r.offset_chunk_num * chunk_size + _r.offset
  _r.offset_chunk_num = parseInt(offset / chunk_size);
  _r.offset = offset - chunk_size * _r.offset_chunk_num;

  // max reading size
  _r.size = size;

  // text data will be automatically stringified with encode.
  _r.encode = options.encode || null;

};
util.inherits(_reader, Emitter);

/**
 * 
 */
_reader.prototype.stop = function(byteRead) {
  this._fin = typeof byteRead == 'number' ? byteRead: true;
};

/**
 * 
 */
_reader.prototype.end = function(endCallback) {

  var _r = this, gc = _r.gc;
  var maxChunk = null, read = 0, chunks = new Buffer(0);

  var encode = _r.encode, offsetRemains = _r.offset;
  var emitData = encode == null ? emitDataBuffer: emitDataString;

  // "endCallback" is callback for buffered reading
  if(isFunction( endCallback )) {

    _r.on(DataEvent.Error, endCallback);

    _r.on(DataEvent.Data, function(n, buf) {
      chunks = Buffer.concat([chunks, buf]);
    }).on(DateEvent.End, function(byte) {
      var r = encode == null ? chunks: chunks.toString(encode);
      endCallback.call(gc, null, byteRead, r);
    });

  }

  // execute reading process
  gc.countChunks(countcb);

  function countcb(er, cnt) {

    if(er)
      return error(er);

    if(!cnt)
      return error(new Error('No chunk data for ' + gc.filename
        + ', filesId is Object("' + gc.filesId + '"), cnt: ' + cnt));

    maxChunk = cnt - 1, process.nextTick(function() {
      _nth(_r.offset_chunk_num);
    });

  }

  function _nth(n) {

    gc.nthChunk(n, nthcb);

    function nthcb(err, chunk_origin) {

      if(err)
        return error(err);

      var chunk = chunk_origin.data.buffer;
      var chunk_len = chunk_origin.length();

      if(offsetRemains > 0) {
        if(offsetRemains >= chunk_len)
          return offsetRemains -= chunk_len, callNext();
        chunk_len -= offsetRemains, chunk = chunk.slice(offsetRemains);
        offsetRemains = 0;
      }

      read += chunk_len;

      var over = _r.size > 0 ? read - _r.size: false;
      var fin = n >= maxChunk || (over && over >= 0);
      if(over > 0) { // read over, to finish
        read -= over;
        chunk = chunk.slice(0, chunk_len - over);
      }

      emitData(n, chunk);

      if(_r._fin != null) // if stop signal received
        fin = true, typeof _r._fin === 'number' && (read = _r._fin);
      process.nextTick(fin ? finish: callNext);

    };

    function callNext() {
      _nth(n + 1)
    }

  }

  function emitDataBuffer(n, chunk) {
    _r.emit(DataEvent.Data, n, chunk);
  }

  function emitDataString(n, chunk) {
    _r.emit(DataEvent.Data, n, chunk.toString(_r.encode));
  }

  function error(err) {
    _r.emit(DataEvent.Error, err);
    careLeak();
  }

  function finish() {
    _r.emit(DataEvent.End, read);
    careLeak();
  }

  function careLeak() {
    setImmediate(function() {
      delete _r.gc, _r.removeAllListeners();
      _r = gc = chunks = emitData = null;
    });
  }

};
//<< ------------ _reader : chunks reading class ------------ <<

Commands.prototype.read = function(size, offset, callback) {
  var gc = this;

  var readOpt = {
    size: null,
    offset: gc.internalPosition || null
  };

  switch(true) {
  
  case isFunction( size ):
    callback = size;
    break;
    
  case isFunction( offset ):
    readOpt.size = size;
    callback = offset;
    break;
    
  default:
    readOpt.size = size;
    readOpt.offset = offset;
    
  }

  return new _reader(gc, readOpt).end(callback);
};

Commands.prototype.readChunks = function(readOpt, callback) {
  callback(null, new _reader(this, readOpt), this);
};

/**
 * Writes some data. This method will work properly only if
 * initialized with mode "w" or "w+".
 */
Commands.prototype.write = function(data, options, callback) {

  var gc = this;
  var gs = gc.gs, gsid = gs.gsid;

  var f_col, EOF, finalClose;

  if(isFunction( options ))
    callback = options, options = {};
  else if(typeof options != 'object')
    options = {};

  var writeCallback = function(er) {
    // try { ... } catch(e) { ... } to avoid the unexpected unlocked finish
    try {
      if(er) throw er; callback(null, gc);
    } catch(e) {
      gc.errHandler(e, callback);
    }
  };

  EOF = options['EOF'] === true ? true: false;
  finalClose = options['finalClose'] === true ? true: false;
  // TODO need pause ?
  // readStream = options['readStream'] = options['readStream'] || false;

  // after the 2nd write 
  // waiting previous write acution in ".writeBuffer()".
  if(gc._writing == FileIs.ON_UPDATE) {
    return runWrite();
  }

  // flag up synchronously.
  gc._writing = FileIs.ON_UPDATE;

  // Wait the last promise and write prepare
  gc.writeStream = gc.writeStream.then(function() {
    return new Promise(writePrepare)['catch'](writeCallback);
  });

  runWrite();
  return;

  function writePrepare(rsl, rej) {

    // the first writing
    // create an data to filesCollection
    gc.filesCollection(function(er, filesCollection) {

      if(er)
        return writeCallback(er);

      f_col = filesCollection;
      f_col.findOne({

        filename: gc.filename,
        _no: gc._no

      }, function(er, obj) {

        // unexpected findOne error
        if(er)
          return writeCallback(er);

        // create new MongoObject
        // !obj || obj._writing != FileIs.ON_UPDATE
        gc.buildMongoObject(FileIs.ON_UPDATE, newMongoObject);

      });
    });

    function newMongoObject(er, monObj) {
      if(er)
        return writeCallback(er);
      f_col.save(monObj, function(er) {

        if(er)
          return writeCallback(er);

        // TODO if w+, copy chunks and set the last chunk to here.
        gc.currentChunk = new Chunk(gc, {
          'n': 0
        });

        rsl();

      });
    }
  }

  function runWrite() {
    // Check if we are trying to write a buffer and use the right method
    gc.writeBuffer(Buffer.isBuffer(data) ? data: new Buffer(data), EOF, finalClose, writeCallback);
  }

};

Commands.prototype.writeBuffer = function(buffer, EOF, finalClose, callback) {

  var gc = this;
  // On occuring error case, "gc.gs" become null.
  // var gs = gc.gs, gsid = gs.gsid;

  if(isFunction( EOF ))
    callback = EOF, finalClose = EOF = false
  else if(isFunction( finalClose ))
    callback = finalClose, finalClose = false;

  var writeBufferCallback = function(er) {
    callback(er, gc)
  };

  gc.writeStream = gc.writeStream.then(function() {
    return new Promise(writeProcessor)
      .then(writeBufferCallback, writeBufferCallback);
  });
  return;

  function writeProcessor(rsl, rej) {

    convertToChunks();

    function convertToChunks(leftOverData) {

      if(!gc.currentChunk)
        return rej(new Error(gc.filename + " not opened."));

      if(OpenModeIs.read(gc.mode))
        return rej(new Error(gc.filename + " not opened for writing"));

      if(leftOverData)
        buffer = leftOverData;

      // Data exceeds current chunk remaining free size; 
      // fill up current chunk and write the rest to a new chunk (recursively)
      var currentChunkNumber = gc.currentChunk.position();
      var leftOverDataSize = gc.currentChunk.chunkSize
        - gc.currentChunk.internalPosition;

      // data slice
      var writeChunkData = null;

      if(buffer.length > leftOverDataSize) {
        writeChunkData = buffer.slice(0, leftOverDataSize);
        leftOverData = buffer.slice(leftOverDataSize);
      } else {
        writeChunkData = buffer;
        leftOverData = null;
      }

      // Let's save the current chunk and then call write again for the remaining data
      // synchronous function "chunk.write()"
      var chunk = gc.currentChunk.write(writeChunkData);

      if(leftOverData) {
        gc.length += chunk.length();
        return chunk.save(leftDataSCB);
      }

      if(EOF === true) {
        gc.length += chunk.length();
        return chunk.save(eofSCB);
      }

      eofSCB();

      function leftDataSCB(er) {

        // TODO error handling
        if(er) {
          console.error('leftDataSCB error: ', gc.filename, er);
          rej(er);
          return;
        }

        gc.currentChunk = new Chunk(gc, {
          'n': ++currentChunkNumber
        });

        // call again for the remaining data in asynchrony
        process.nextTick(convertToChunks.bind(null, leftOverData));

      }

      function eofSCB(er) {

        if(er) {
          console.error('eofSCB error: ', gc.filename, er);
        }

        if(finalClose === true)
          gc.close();
        rsl();

      }

    }
  }

};

/**
 * 
 */
Commands.prototype.setMongoObjectToSelf = function(monObj) {

  var gc = this;

  // file is found
  gc.filesId = monObj._id;
  //  gc.filename = monObj.filename;
  gc.internalChunkSize = monObj.chunkSize;

  gc.uploadDate = monObj.uploadDate;
  gc.contentType = monObj.contentType;
  gc.length = monObj.length || 0;
  gc.internalMd5 = monObj.md5;

  // not in ./mongofiles put object

  // TODO check : not in monObj.
  gc.aliases = monObj.aliases;
  gc.metadata = monObj.metadata;

};

/**
 * 
 */
Commands.prototype.buildMongoObject = function(writing, callback) {

  var gc = this;
  var gs = gc.gs, gsid = gs.gsid;

  if(isFunction( writing ))
    callback = writing, writing = FileIs.STABLE;

  var is_object_id = !!gc.filesId && (gc.filesId instanceof gc.ObjectID);
  var mongoObject = {

    '_id': is_object_id ? gc.filesId: new gc.ObjectID(),
    'filename': gc.filename,

    'length': typeof gc.length == 'number' ? gc.length: null,
    'chunkSize': gc.internalChunkSize || gc.defaultChunkSize,

    // SHOULD USE "gc" VALUE PRIMARY!
    'contentType': gc.contentType || gs.contentType,
    'uploadDate': gc.uploadDate || new Date(),
    'transferEncoding': gc.transferEncoding || null,

    // SHOULD USE "gs" VALUE PRIMARY!
    'aliases': gs.aliases || gc.aliases,
    'metadata': gs.metadata || gc.metadata || {},

    '_no': gc._no,
    '_writing': writing

  };

  // Always update "atime" automatically
  mongoObject.metadata.atime = new Date();

  if((gc.currentChunk || '').chunkSize)
    mongoObject['chunkSize'] = gc.currentChunk.chunkSize;

  var md5Command = {
    filemd5: gc.filesId,
    root: gc.root
  };

  gc.db.command(md5Command, function(er, results) {

    if(er == null && results)
      mongoObject.md5 = results.md5;

    gc.setMongoObjectToSelf(mongoObject);

    // Ignore md5Command error
    // "Can't get executor for query" for dropped database.
    callback(null, mongoObject);

  });

};

Commands.prototype.close = function(callback) {

  var gc = this;
  var gs = gc.gs, gsid = gs.gsid;

  var closingCallback = function(er) {
    // try { ... } catch(e) { ... } to avoid the unexpected unlocked finish
    try {
      if(er) throw er; gc.rewind(), gs.close(closeCallback);
    } catch(e) {
      gc.errHandler(e, callback);
    }
  };

  var mode = gc.mode, f_col;

  if(!gc._no)
    return closingCallback();

  if(OpenModeIs.read(mode))
    return closingCallback();

  // update files stats
  gc.filesCollection(function(er, col) {

    if(er)
      return closingCallback(er);

    var _no = gc._no;
    (f_col = col).update({

      filename: gc.filename,
      _no: _no

    }, {
      // new file status
      $set: {

        contentType: gc.contentType,
        uploadDate: new Date(),

        length: gc.length,
        aliases: gc.aliases,
        metadata: gc.metadata,

        _writing: FileIs.STABLE,
        _no: _no

      }

    }, function(er) {
      if(er)
        return closingCallback(er);
      ((_no & 0xffffff) == _no ? deleteOld: resetStat)();
    });

    function resetStat() {
      gc.statsCollection(function(er, s_col) {

        if(er) {
          console.error('(ignored error) resetStat error: ', er);
          return deleteOld();
        }

        // backward file number.
        // max: 0xffffff = 16777215 times write.
        s_col.findAndModify({

          // query
          _id: gc.filename,
          _no: {
            $gte: 0xffffff
          }

        }, [], { // sort | update document

          $set: {
            _no: 1
          }

        }, { // options

          upsert: false

        }, deleteOld);
        // ignore findAndModify error.

      });
    }

    function deleteOld() {

      var delete_condition = {

        filename: gc.filename,

        _writing: {
          $in: [FileIs.STABLE, FileIs.ON_UPDATE],
        },

        _no: {
          $ne: _no
        },

        // possibility of reading
        'metadata.atime': {
          $lt: new Date(Date.now() - Commands.OLD_DATA_READABLE_MILLISECOND)
        }

      };

      gc.deleteFiles(delete_condition, true, function(er) {
        if(er) {
          console.error('(ignored error) deleteOld error: ', er);
        }
        closingCallback();
      });

    }

  });

  function closeCallback(e) {

    // fatal error
    if(e) {
      gs.error(e, callback);
    } else {
      callback(null);
    }

    // memory leak care
    setImmediate(function() {

      if(!gc)
        return;

      // release targets
      var _ = ['_files', '_no'];
      _.push.apply(_, CollectionsList);
      _.push.apply(_, ['gs', 'db', 'ObjectID']);

      // release
      _.forEach(function(k) {
        delete gc[k];
      });

      // release reference
      gc = gs = null;

    });

  } // << close callback
};

/**
 * Gets the nth chunk of this file.
 * 
 * @param chunkNumber
 *        {number} The nth chunk to retrieve.
 * @param callback
 *        {function(*, Chunk|object)} This will be called
 *        after executing this method. null will be passed
 *        to the first parameter while a new {@link Chunk}
 *        instance will be passed to the second parameter if
 *        the chunk was found or an empty object {} if not.
 * 
 */
Commands.prototype.countChunks = function(callback) {
  var gc = this;
  gc.chunksCollection(function(er, c_col) {

    if(er)
      return callback(er);

    c_col.count({
      'files_id': gc.filesId
    }, callback);

  });
};

Commands.prototype.nthChunk = function(chunkNumber, callback) {
  var gc = this;
  gc.chunksCollection(function(err, col) {

    if(err)
      return callback(err);

    col.findOne({
      'files_id': gc.filesId,
      'n': chunkNumber
    }, findcb);

  });
  function findcb(err, chunk) {
    callback(err, new Chunk(gc, chunk == null ? {}: chunk));
  };
};

Commands.prototype.lastChunk = function(callback) {
  var gc = this;

  gc.countChunks(counted);

  function counted(er, cnt) {
    er ? callback(er): gc.nthChunk(--cnt, callback);
  }
};

/**
 * get files metadata.
 * if "meta" set to false, returns files object itself.
 */
Commands.prototype.stat = function(meta, callback) {

  var gc = this;
  var gs = gc.gs, gsid = gs.gsid;

  if(isFunction( meta ))
    callback = meta, meta = false;

  var statCallback = function(er, stat) {
    // try { ... } catch(e) { ... } to avoid the unexpected unlocked finish
    try {
      if(er) throw er; callback(null, stat, gc);
    } catch(e) {
      gc.errHandler(e, callback);
    }
  };

  // before open, cannot get stat
  if(gc._no == null)
    return statCallback(Err.EPERM);

  statCallback(null, meta ? gc.metadata: createStat(gc))
  return;

  // only metadata, or stat object
  // TODO from db, like format as fs.stat
  gc.filesCollection(function(er, f_col) {
    er ? statCallback(er): f_col.findOne({

      filename: gc.filename,
      _writing: FileIs.STABLE

    }, {

      sort: {
        uploadDate: -1
      }

    }, function(er, obj) {

      obj = obj || {};
      // output files status.
      statCallback(er, meta === true ? obj.metadata: createStat(obj));

    });
  });
  function createStat(obj) {
    return {
      length: obj.length,
      contentType: obj.contentType,
      uploadDate: obj.uploadDate,
      metadata: obj.metadata,
      md5: obj.md5
    };
  }

};

/**
 * The only thing that this function do, delete 
 * "FileIs.STABLE" status file/chunks
 */
Commands.prototype.deleteFile = function(callback) {
  var gc = this;
  gc.deleteFiles({

    filename: gc.filename,
    _writing: FileIs.STABLE

  }, true, callback);
};

/**
 * Deletes the files of this file in the database.
 * 
 * @returns err (Error), res (Array): deleted files datas to
 *          delete others
 */
Commands.prototype.deleteFiles = function(key, chunks_del, callback) {

  var gc = this;

  var f_col = null, f_sel = {};
  var rerr = null, res = [];

  if(isFunction( key ))
    callback = key, chunks_del = false, key = gc.filesId;
  else if(isFunction( chunks_del ))
    callback = chunks_del, chunks_del = false;

  var delCallback = function(er) {
    // try { ... } catch(e) { ... } to avoid the unexpected unlocked finish
    try {
      if(er) throw er; callback(null, gc);
    } catch(e) {
      gc.errHandler(e, callback);
    }
  };

  // key has 3 patterns acceptable.
  if(key instanceof gc.ObjectID) {
    // files_id
    f_sel._id = key;
  } else if(typeof key == 'string') {
    // an filename
    f_sel.filename = key;
  } else {
    // selector
    f_sel = key;
  }

  gc.filesCollection(function(er, filesCollection) {
    if(er)
      return delCallback(er);
    (f_col = filesCollection).find(f_sel, findcb);
  });

  function findcb(er, cur) {

    if(er) {
      return cur = null, delCallback(er);
    }

    var prms = [];
    cur.each(function(er, doc) {

      if(er) {
        cur.rewind(), cur = null;
        delCallback(er);
        return;
      }

      if(!doc) {
        cur.rewind(), cur = null;
        Promise.all(prms).then(function() {
          delCallback(); // DON'T CALL DIRECT. PROMISE.ALL RETURNS "[]"
        })['catch'](delCallback);
        return;
      }

      var _id = doc._id;
      prms.push(new Promise(function(rsl, rej) {
        f_col.remove({
          _id: _id
        }, {
          w: 1
        }, function(er) {

          // if error occurs, set as return error.
          if(er) {
            return rej(er);
          }

          gc.deleteChunks(_id, function(er) {
            er ? rej(er): rsl();
          });

        });
      }));

    });
  } // << findcb()

};

/**
 * Deletes all the chunks of this file in the database.
 */
Commands.prototype.deleteChunks = function(f_id, callback) {

  var gc = this;

  if(isFunction( f_id ))
    callback = f_id, f_id = gc.filesId;

  gc.chunksCollection(function(err, c_col) {
    c_col.remove({
      'files_id': f_id
    }, {
      w: callback ? 1: 0
    }, callback);
  });

};

/**
 * Get fs collections.
 */
for( var name in Collections) {
  (function(fname, space) {
    Commands.prototype[fname] = function(callback) {

      var gc = this, k = '_' + fname;
      if(gc[k])
        return callback(null, gc[k]);

      gc.db.collection(this.root + space, collcb);

      function collcb(err, collection) {
        err ? callback(err): callback(null, gc[k] = collection);
      }

    };
  })(name + 'Collection', Collections[name]);
} // << ---------------- each collection getter prototypes ---------------- <<

/**
 * 
 */
Commands.prototype.rewind = function() {
  this.length = 0, this.uploadDate = null;
  this.writeStream = Promise.resolve();
};

/**
 * Stores a file from the file system to the GridFS
 * database.
 */
Commands.prototype.chunkIn = function(options, callback) {

  var gc = this, gs = gc.gs;

  // local variables
  var filepath = null, contents = null, size = 0;

  // arguments initialize
  if(isFunction( options ))
    callback = options, options = {};
  else if(typeof options != 'object')
    options = {};

  filepath = options.filePath || gs.internalFilePath;
  contents = options.contents;

  if(OpenModeIs.read(gc.mode))
    return gc.errHandler('ChunkIn requires mode "w" or "w+".', callback);

  var ee = options.emitter || new Emitter();
  var poser = null;

  // memory leak care
  var careLeak = function() {
    setImmediate(function() {

      ee && ee.removeAllListeners();
      ee = gc = gs = null;

    });
  };

  ee.on('error', careLeak).on('end', careLeak);

  // set callback caller
  if(isFunction( callback ))
    ee.on('error', last).on('end', last);

  if(options.status) {
    // exec after waiting user's event binding operation
    process.nextTick(function() {
      afterStat(null, options.status);
    });
  } else {
    // in this case, file must be exist in  OS FileSystem
    fs.stat(filepath, afterStat);
  }

  return ee;

  function afterStat(er, stats) {

    if(er)
      return gc.errHandler('stats failed for ' + filepath, er, ee);

    // must be write for update/copy files data.
    if(!(stats || '').size)
      contents = contents || '';

    (contents == null ? readStreamAndWrite: writeContent)();

  } // << -------- afterStat() --------

  function writeContent() {
    writeStreamChunk(contents, true);
  } // << -------- writeContent() --------

  function readStreamAndWrite() {

    var stream = (options.stream || fs.createReadStream(filepath, {
      bufferSize: gc.internalChunkSize
    }));

    var careLeak = function() {
      setImmediate(function() {

        if(!stream)
          return;

        if(stream.removeAllListeners)
          stream.removeAllListeners();

        stream = null;

      });
    };

    var is_stream = isFunction( stream.pause );
    stream.on('error', function(e) {

      gc.errHandler('write failed ' + filepath, e, ee);
      careLeak();

    }).on('data', function(buf) {

      // breathing for another request
      if((size += buf.length) >= gc.defaultChunkSize) {

        // "stream" is readable-stream or event-emitter instance.
        if(is_stream) {
          stream.pause();
        }
        timer.setTimeout(function() {

          // slight breathing for the large file writing.
          size -= gc.defaultChunkSize;
          if(is_stream) {
            stream.resume();
          }

        }, parseInt(120 * Math.random()));

      }
      writeStreamChunk(buf)

    }).on('end', function() {

      writeStreamChunk(new Buffer(0), true);
      careLeak();

    }).emit('ready');

  } // << -------- readStreamAndWrite() --------

  function writeStreamChunk(buf, is_last) {
    gc.write(buf, {

      EOF: is_last

    }, function(er) {

      if(er)
        return ee.emit('error', er);

      ee.emit('data', gc.currentChunk.chunkNumber, buf);
      !is_last || ee.emit('end');

    });
  } // << -------- writeBuffer() --------

  function last(er) {
    callback(er, gc);
  }

};

/**
 * Save
 */
Commands.prototype.writeFile = function(options, callback) {
  var gc = this;

  // options and callback are optional.
  if(isFunction( options ))
    callback = options, options = {};
  else if(typeof options != 'object')
    options = {};

  /* automatically set in chunkIn()
  if(!options.filePath)
    options.filePath = this.gs.internalFilePath;
  */

  return gc.chunkIn(options, callback);
};

/**
 * Readable Old Data Millisecond.
 * 
 * @constant
 */
Commands.OLD_DATA_READABLE_MILLISECOND = 3 * 60 * 1000; // 3 min

// ---------------------------------------------------------- //
// Appendix
// ---------------------------------------------------------- //

// TODO use "util.debuglog()" and node natives
// var debuglog = util.debuglog('mongodb.gridfs');

//debug judgement
var Debug = process.env.NODE_DEBUG, DebugMatch = null, DebugLevel = 0;
Debug = typeof Debug == 'string' ? Debug: String(Debug || '');

Debug = 'true' == Debug || !!Debug && 'false' != Debug;
DebugMatch = Debug && /(^|,)mongodb(.gridfs)?(=v{0,5})?/.test(Debug);
DebugLevel = DebugMatch && (DebugMatch[3] || '').length || 0;

var OutDebug = Debug ? function(level, fn, message) {
  level <= DebugLevel && OutMessage('debug', fn, message);
}: Function();

var OutMessage = function(type, fn, message) {

  if(arguments.length == 2)
    message = fn, fn = type;

  message = OutMessage.message(fn, message);
  switch(type) {

  case 'error':
    if(Array.isArray(message))
      console.error.apply(console, message);
    else
      console.error(message);
    return;

  default:
    if(Array.isArray(message))
      console.log.apply(console, message);
    else
      util.log(message);
    return;

  }

};

OutMessage.message = function(fn, mess) {
  var prefix = '[gridstore_commands.js:' + fn + '()] ';
  mess = isFunction( mess ) ? mess(): mess || '';

  if(Array.isArray(mess))
    mess.unshift(prefix)
  else
    mess = prefix + mess;

  return mess;
};
//<< console logging function <<

function errCodeGen(err, num, mes) {
  return errGen(err + '(' + num + '): ' + mes);
}

function errFnGen(_mes, _mtd) {

  return function(mes, mtd) {
    if(_mes && mes)
      mes = _mes + mes;
    if(_mtd && mtd)
      mtd = mtd + _mtd;
    return errGen(mes || _mes, mtd || _mtd);
  };

}

function errGen(mes, method) {
  return 'Error: ' + (mes ? mes + ' ': '')
    + (method ? 'at ' + method + '()': '') + ' in gridCommands.';
}

/**
 * 
 */
function isFunction(x){
  return typeof x == 'function';
}
function isErrorObject(x) {
  return x instanceof Error;
}

