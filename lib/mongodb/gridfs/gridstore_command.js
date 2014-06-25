/***/
var fs = require('fs'), util = require('util');
var Emitter = require('events').EventEmitter, Chunk = require('./chunk').Chunk;

//debug function set >>
var debug = process.env.NODE_DEBUG, DEBUG_CATEGORY = 'mongodb-native';
var FILE_NAME = __filename.split('/').slice(-1).toString().replace('.js', '');
if(debug
  && (debug == 'true' || (new RegExp(DEBUG_CATEGORY + '|' + FILE_NAME)
      .test(debug)))) {
  debug = function(s) {
    util.error(FILE_NAME + '.js : ' + (typeof s == 'function' ? s(): s));
  };
} else {
  debug = function() {
  };
}
// << function debug() is available.

// console logging function >>
var OutMessage = function(type, fn, message) {
  // TODO save in database
  util[type](OutMessage.message(fn, message));
};
OutMessage.message = function(fn, message) {
  return '[gridstore_commands.js:' + fn + '()] ' + message;
};
OutMessage.log = function(fn, message) {
  OutMessage('log', fn, message);
};
// <<

// constants
var Const = {
  NewLock: 'newlock'
};

// actions 
var Action = {
  READ: 'READ',
  WRITE: 'WRITE'
};

// stat status
var StatIs = {
  OPEN: 0,
  LOCKED: 1
};

// file status
var FileIs = {
  STABLE: 0,
  BEFORE_STABLE: 1,
  ON_UPDATE: 2,
  BEFORE_DELETE: 3
};

// Referenced
var REFERENCE_BY = {
  ID: 0,
  NAME: 1
};

// emitted by _reader
var DataEvent = {
  Data: 'data',
  Error: 'error',
  End: 'end'
};

var Err = {
  ENOENT: errCodeGen('ENOENT', 2, 'No such file'),
  EEXIST: errCodeGen('EEXIST', 17, 'File already exists'),
  colOpen: errFnGen('Collection open failed.', 'Collection.open'),
  colFind: errFnGen('Collection find failed.', 'Collection.find'),
  colUpdate: errFnGen('Collection update failed.', 'Collection.update'),
  statFind: errFnGen('Lockstat find failed.', 'getStatsData'),
  lockUnexp: errFnGen('Unexpected in locking.'),
  lockExist: errFnGen('Another eventLock '),
  lockRelFail: errFnGen('Unexpected lock release failure.')
};

function errCodeGen(err, num, mes) {
  return errGen(err + '(' + num + '): ' + mes);
};

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

/**
 * GridStoreCommands
 */
module.exports = Commands;

function Commands(gridstore) {

  var self = this;

  // inherits from gridstore
  this.gs = gridstore;

  this.db = this.gs.db;
  this.root = this.gs.root;
  this.referenceBy = this.gs.referenceBy;
  this.filesId = this.gs.filesId;
  this.filename = this.gs.filename;
  this.defaultChunkSize = this.gs.defaultChunkSize;
  this.contentType = this.gs.contentType;
  this.mode = this.gs.mode;
  this.safe = this.gs.safe;

  // stats
  this._stats = {
    begin: null,
    end: null
  };

  // files
  this._files = {
    forRead: null,
    forWrite: null
  };

  // internal position for chunk reading / writing
  this.internalPosition = 0;

  // self event emitter chunk writing
  this.writing = false, this.waiting = [], this.incomingDataLength = 0;

  // memory my latest action
  this._latest = {};

};

Commands.prototype.writeEnqueue = function(fn) {

  var q = this.waiting;

  q.push(fn);
  if(q.length == 1)
    q[0]();

};

Commands.prototype.writeDequeue = function() {

  // buffer size and chunk size is not match, so that we shoud
  // control the order of writing
  var q = this.waiting;

  q.shift();

  if(q.length)
    return q[0]();

};

Commands.prototype.seek = function(pos) {

  this.internalPosition = pos;

};

Commands.prototype.default_selector = function(writing, ref) {

  var selector = {}, id = null, name = null;

  if(typeof ref == 'number') {

    if(ref == REFERENCE_BY.ID)
      id = this.filesId;

    else
      name = this.filename;

  } else {

    if(ref instanceof this.db.bson_serializer.ObjectID)
      id = ref;

    else if(typeof ref == 'string')
      name = ref;

    else
      id = this.filesId, name = this.filename;

  }

  if(writing) {

    if(Array.isArray(writing)) {
      var wsel = {};
      wsel['$in'] = writing;
      selector['_writing'] = wsel;
    } else {
      selector['_writing'] = writing;
    }

  }

  if(id)
    selector._id = id;

  else if(name)
    selector.filename = name;

  return selector;

};

Commands.prototype.errHandler = function(msg, err, handle) {

  var self = this;

  if(typeof err == 'function' || err instanceof Emitter)
    handle = err, err = null;

  var filemsg = this.filename ? '(filename: ' + this.filename + ' )': '';
  msg = msg ? msg + ":\n": '';

  if(err)
    err.message = msg + filemsg + "" + err.message;
  else
    err = new Error(msg + filemsg);

  // CLC
  // console.log(err.message);

  var afterClose = null;
  if(typeof handle == 'function')
    afterClose = function(e) {
      handle(err);
    };
  else if(handle instanceof Emitter)
    afterClose = function(e) {
      handle.emit('error', err);
    };

  // release lock
  this.locktime ? this.eventUnlock(afterClose): afterClose();

};

Commands.prototype.open = function(callback) {

  var self = this, filesCol = null, r_stats = null, mongoObject = null;
  var r_releaseHandler = null, w_releaseHandler = null;

  begin();

  function begin() {
    self.existStatsData({}, checkExist);
  }

  function checkExist(err, exist) {

    if(err)
      util.error(err);

    //    if(/file-io\/index.js/.test(self.filename))
    //      util.log('Stat: ' + JSON.stringify(exist));

    exist ? self.filesCollection(fcollcb): self.initStatsData({

      _id: self.filename,
      _no: 0,
      READ: {},
      WRITE: {},
      update: null,
      _writing: StatIs.OPEN

    }, begin);

  }

  function fcollcb(err, filesCollection) {

    filesCol = filesCollection;

    if(err)
      return self.errHandler(Err.colOpen('', 'files'), err, callback);
    (OpenModeIs.read(self.mode) ? readEventLock: writeEventLock)();
  }

  function readEventLock() {

    self.eventLock(Action.READ, Const.NewLock, function(err, stats, release) {

      //      if(!stats)
      //        debugger;

      r_stats = stats, r_releaseHandler = release;

      if(err)
        return self.errHandler(Err.lockUnexp('Read Lock failed.', 'open'), err,
          callback);

      //      if(/file-io\/index.js/.test(self.filename))
      //        util.log('Get Read Lock Succeeded: ' + JSON.stringify(stats));

      getFilesObject(null, upsetReadLock);

    });

  }

  function getFilesObject(conds, callback) {

    // get all files data to check circumstances
    filesCol.find(self.default_selector(conds, REFERENCE_BY.NAME), {}, {

      slaveOk: true

    }, function(err, cur) {

      var obj = null, del = false;
      cur.each(function(err, data) {

        if(err || !data)
          return self.delete_flag = del, callback(err, obj);

        if(data._writing == FileIs.STABLE)
          return !del && !obj ? obj = data: '';

        if(data._writing == FileIs.BEFORE_STABLE)
          return !del ? obj = data: '';

        if(data._writing == FileIs.ON_UPDATE)
          return;

        if(data._writing == FileIs.BEFORE_DELETE)
          return del = true, obj = null;

      });

    });

  }

  function upsetReadLock(err, obj) {

    if(err)
      return self.errHandler(Err.colFind('', 'files'), err, callback);

    // TODO need "|| {}" ?
    var sets = {}, _la = self._latest[Action.READ] || {}, lock_key = _la._id;
    sets[Action.READ] = (r_stats || {})[Action.READ];

    delete sets[Action.READ][lock_key];

    if(obj) {
      // TODO to function for making _id value
      _la._id = obj._id.toString() + '-' + r_stats._no.toString();
      sets[Action.READ][_la._id] = _la.time;
    } else {
      delete self._latest[Action.READ], delete self.locktime;
    }

    self.upsetStatsData({
      _id: self.filename,
      _writing: StatIs.LOCKED
    }, sets, r_stats._no, function() {

      // TODO why r_releaseHandler occationally "undefined"?
      // release read resource.
      typeof r_releaseHandler == 'function' ? r_releaseHandler(function() {
        assignAndJudge(obj);
      }): assignAndJudge(obj);

    });

  }

  function assignAndJudge(obj) {

    if(OpenModeIs.read(self.mode) && !obj) {
      // [2014.6.24] not to leak fs.condition data
      self.errHandler(Err.ENOENT, callback);

      setTimeout(function() {
        self.getStatsData(function(err, stats) {
          if(err || stats && stats.update != null)
            return;
          self.removeStatsData(function() {
            // noop ?
          });
        });
      }, 800);
      return;

    }

    if(obj) // stable fs.files object
      self._files.forRead = obj;

    if(OpenModeIs.overwrite(self.mode) && self.safe && obj) {
      delete self._files.forRead;
      return self.errHandler(Err.EEXIST, callback);
    }

    if(OpenModeIs.read(self.mode)) {
      self.setMongoObjectToSelf(obj);
      return self.chunksCollection(ccolcb);
    }

    // the good end for "write" open.
    callback.call(self);

  }

  function ccolcb(err, chunksCollection) {

    if(err != null)
      return self.errHandler('filesCollection in open gridCommands.', err,
        callback);

    // (read mode) get chunks collection 
    return self.countChunks(forReadEnd);

  }

  function forReadEnd(err, cnt) {

    /* no chunk data file exists. 
    if(err || cnt < 1)
      return self.errHandler('No chunks data in open gridCommands.', err,
        callback);
    */

    if(err)
      return self.errHandler('Chunks read error in open gridCommands.', err,
        callback);

    // the good end for "read" open.
    callback.call(self);

  }

  // TODO check the mean for serverConfig.primary
  // if(/^w\+?$/.test(self.mode) && self.db.serverConfig.primary != null)

  function writeEventLock() {

    // set new fs.files with writing = 1.
    self.filesId = self.uploadDate = self.length = null;
    self.buildMongoObject(FileIs.ON_UPDATE, eventLockId);

  }

  function eventLockId(monObj) {

    // lock on node for writing object
    self.eventLock(Action.WRITE, monObj._id, function(err, stats, release) {

      w_releaseHandler = release, mongoObject = monObj;

      if(err)
        return self.errHandler(Err.lockUnexp('Write Lock failed.', 'open'),
          err, callback);

      filesCol.count(self.default_selector([FileIs.BEFORE_STABLE,
        FileIs.ON_UPDATE], REFERENCE_BY.NAME), countcb);

    });

  }

  function countcb(err, cnt) {

    if(cnt) // writing process is in progress ( is there such a situation ? )
      return w_releaseHandler(waitProcess);
    return self.upsertFilesData(mongoObject, savedcb);

  }

  function savedcb(err) {

    w_releaseHandler(function() {
      filesCol.findOne(self.default_selector(FileIs.ON_UPDATE), {
        slaveOk: false
      }, getsavedcb);
    });
    // note: "slaveOk:false" is no effect because of collection.js line:767.
    //       This mean is to represent that this "findOne" must execute on
    //       "slaveOk:false" mode because of the short time between save and
    //       find.

  }

  function getsavedcb(err, obj) {

    if(err)
      return self.errHandler(Err.colFind('new fs.files failure.', 'files'),
        err, callback);

    if(!obj) // save incomplete because of unique index
      return waitProcess();

    self._files.forWrite = obj;
    (OpenModeIs.overwrite(self.mode) ? setChunk: setCursor)();

  }

  function setCursor() {

    self.lastChunk(setChunk);

  }

  function setChunk(err, chunk) {

    self.length = 0;
    self.currentChunk = chunk instanceof Chunk ? chunk: new Chunk(self, {
      'n': 0
    });

    if(err)
      return self.errHandler(Err.colFind('lastChunk failure.', 'files'), err,
        callback);

    // the good end for write open. ( callback after read object checking )
    getFilesObject([FileIs.STABLE], function(err, obj) {
      assignAndJudge(obj);
    });

  }

  function waitProcess() {

    // waiting log
    OutMessage.log('waitProcess', 'Enter condition; '
      + self.filename
      + ' (writingChunk stats='
      + (self._waitingChunk ? self._waitingChunk.obj._id + ':'
        + self._waitingChunk.cnt: 'undefined') + ')');
    // ex: [gridfs] enter waitProcess(): (writingChunk stats=undefined:0)

    if(!self._waitingChunk)
      self._waitingChunk = {};

    self.filesCollection(function(err, col) {

      col.findOne(self.default_selector(
        [FileIs.BEFORE_STABLE, FileIs.ON_UPDATE], REFERENCE_BY.NAME), {
        slaveOk: false
      }, wpFindcb);

    });

  }

  function wpFindcb(err, obj) {

    if(err)
      return self.errHandler('Find failure on waitPrcess.', err, callback);

    // if not exist neither BEFORE_STABLE nor ON_UPDATE file,
    if(!obj)
      return endOfWait();

    // when first wait, set waiting circumstances
    if(Object.keys(self._waitingChunk).length == 0) {
      self._waitingChunk.obj = obj;
      self._waitingChunk.cnt = null;
    }

    // set files condition, just now.
    self._waitingChunk.objNow = obj;

    // find chunks condition
    self.chunksCollection(function(err, col) {
      col.count({
        'files_id': obj._id
      }, wpCountcb);
    });

  }

  function wpCountcb(err, cnt) {

    if(err)
      return endOfWait(err);

    if(!cnt)
      cnt = 0;

    var wc = self._waitingChunk;
    self.getStatsData(function(err, stats) {

      if(err)
        return endOfWait(err);

      var el = stats[Action.WRITE];
      // if unexpected stats data, then throw error
      if(!el || typeof el != 'object')
        return endOfWait(new Error(OutMessage.message('waitProcess',
          'Unexpected WRITE value; ' + el)));

      // get writing files_id
      var el_id = (Object.keys(el)[0] || '').substr(0, 24);

      // writing have already finished
      if(!el_id)
        return endOfWait();

      // regular circumstances
      /* 1. first wait 
       *    (wc.cnt == null)
       * 2. on writing chunk is changed 
       *    (wc.cnt != cnt)
       * 3. writing cluster is changed (self or other process, usually not mine)
       *    (el_id === obj._id == wc.is_mine)
       * 3-1[is_mine == false]. on writing object is changed (other proces)
       *    (obj._id != objNow._id)
       * 3-2[is_mine == true]. on writing object is changed (self)
       *    // rare case
       *    (el_id != objNow._id)
       */

      if(wc.cnt == null || wc.cnt != cnt) // 1 and 2
        return setWaitingTimeout();

      // set whether lock is mine or other process
      var is_mine_now = el_id === wc.obj._id.toString();
      if(wc.is_mine == null)
        wc.is_mine = is_mine_now;

      if(wc.is_mine != is_mine_now) // 3
        return setWaitingTimeout();

      if(wc.is_mine === false) {
        if(wc.obj._id.toString() != wc.objNow._id.toString()) // 3-1
          return setWaitingTimeout();
      } else {
        if(el_id != wc.objNow._id.toString()) // 3-2
          return setWaitingTimeout();
      }

      // exec recover
      self.recover(wc.objNow._id, waitProcess);

      function setWaitingTimeout() {
        OutMessage.log('waitProcess', 'setTimeout condition; WritingID: '
          + el_id + ' for ' + self.filename);
        wc.cnt = cnt, wc.objNow = wc.obj;
        setTimeout(waitProcess, Commands.DEFAULT_SAVE_RETRY_MILLISECOND);
      }

    });

  }

  function endOfWait(err) {
    err ? fcollcb(err): self.filesCollection(fcollcb);
    delete self._waitingChunk;
  }

};

Commands.prototype.initStatsData = function(stats, callback) {

  this.statsCollection(function(err, statsCollection) {

    if(err)
      return callback(err);

    statsCollection.insert(stats, callback);

  });

};

Commands.prototype.existStatsData = function(sel, callback) {

  var self = this;

  this.statsCollection(function(err, statsCollection) {
    if(err)
      return callback(err);
    sel._id = self.filename;
    statsCollection.findOne(sel, callback);
  });

};

Commands.prototype.getStatsData = function(callback) {

  var self = this;

  this.statsCollection(function(err, statsCollection) {
    if(err)
      return callback(err);
    statsCollection.findOne({
      _id: self.filename
    }, callback);
  });

};

Commands.prototype.removeStatsData = function(callback) {
  var self = this;
  this.statsCollection(function(err, statsCollection) {
    err ? callback(err): statsCollection.remove({
      _id: self.filename
    }, callback);
  });
};

Commands.prototype.upsetStatsData = function(criteria, stats, no, callback) {

  var self = this;

  this.statsCollection(function(err, statsCollection) {

    if(err)
      return callback(err);

    var val = {
      $set: stats
    };

    if(no === true)
      // lock
      val.$inc = {
        _no: 1
      };

    else if(typeof no == 'number')
      // unlock
      stats._no = no;

    //    console.log('UPSET:');
    //    console.log(JSON.stringify(criteria));
    //    console.log(JSON.stringify(val));
    //    console.log('======');

    statsCollection.update(criteria, val, {
      safe: typeof callback == 'function'
    }, callback);

  });

};

Commands.prototype.upsertFilesData = function(monObj, callback) {

  this.filesCollection(function(err, filesCollection) {

    if(err)
      return callback(err);

    filesCollection.save(monObj, {
      safe: true
    }, callback);

  });

};

Commands.prototype.upsetFilesData = function(id, obj, callback) {

  this.filesCollection(function(err, filesCollection) {

    if(err)
      return callback(err);

    filesCollection.update({
      _id: id
    }, {
      $set: obj
    }, {
      safe: true
    }, callback);

  });

};

// =========== [START] Reader : chunk read class ============ //
var _reader = function(gc, options) {

  otpions = options || {}
  var offset = options.offset || 0, chunk_size = gc.internalChunkSize
    || gc.defaultChunkSize;

  this.gc = gc;
  this.offset_chunk_num = parseInt(offset / chunk_size);
  this.offset = offset - chunk_size * this.offset_chunk_num;
  this.size = parseInt(options.size || null);
  this.encode = gc.gs.contentType.slice(0, 4) == 'text' ? options.encode
    || 'utf8': null;

  this._emitter = new Emitter();

};

_reader.prototype.on = function(eve, fn) {

  var evs = DataEvent.Data + '|' + DataEvent.Error + '|' + DataEvent.End;
  if(!new RegExp(evs).test(eve))
    throw new Error('No Such Event : ' + eve);

  return this._emitter.on(eve, fn), this;

};

_reader.prototype.end = function(endCallback) {

  var self = this, gc = this.gc;
  var maxChunk = null, read = 0, chunks = new Array();
  var offsetRemains = this.offset;

  var emitData = this.encode == null ? emitDataBuffer: emitDataString;

  this.on('data', function() {

    // update condition data
    var val = {}, _la = gc._latest[Action.READ];
    val[Action.READ + '.' + _la._id] = Date.now();

    // to notice alive this process
    gc.upsetStatsData({
      _id: gc.filename,
      _writing: StatIs.LOCKED
    }, val);

  });

  // "endCallback" is callback for buffered reading
  if(typeof endCallback == 'function')
    this.on(DataEvent.Error, function(e) {
      endCallback(e);
    }).on(DataEvent.Data, function(n, buf) {
      self._chunks.push(buf);
    }).on(DateEvent.End, onEnd);

  // execute reading process
  gc.countChunks(countcb);

  function onEnd(byteRead) {
    endCallback.call(gc, null, byteRead, self.encode == null ? Buffer
        .cocat(chunks): chunks.join(''))
  }

  function countcb(err, cnt) {

    if(err)
      return finish(err);

    if(cnt == 0)
      return finish(new Error('No chunk data for ' + gc.filename));

    maxChunk = cnt - 1, process.nextTick(function() {
      _nth(self.offset_chunk_num);
    });

  }

  function _nth(n) {

    gc.nthChunk(n, nthcb);

    function nthcb(err, chunk_origin) {

      if(err)
        return error(err);

      var chunk = chunk_origin.binary.buffer;
      var chunk_len = chunk_origin.length();

      if(offsetRemains > 0) {
        if(offsetRemains >= chunk_len)
          return offsetRemains -= chunk_len, callNext();
        chunk_len -= offsetRemains, chunk = chunk.slice(offsetRemains);
        offsetRemains = 0;
      }

      read += chunk_len;

      var over = self.size > 0 ? read - self.size: false;
      var fin = n >= maxChunk || (over && over >= 0);
      if(over > 0) { // read over, to finish
        read -= over;
        chunk = chunk.slice(0, chunk_len - over);
      }

      emitData(chunks.push(chunk) - 1, chunk);
      process.nextTick(fin ? finish: callNext);

    };

    function callNext() {
      _nth(n + 1)
    }

  }

  function emitDataBuffer(n, chunk) {
    self._emitter.emit(DataEvent.Data, n, chunk);
  }

  function emitDataString(n, chunk) {
    self._emitter.emit(DataEvent.Data, n, chunk.toString(self.encode));
  }

  function error(err) {
    self._emitter.emit(DataEvent.Error, err);
  }

  function finish() {
    self._emitter.emit(DataEvent.End, read);
  }
};
//=========== [END] Reader : chunk read class ============ //

Commands.prototype.read = function(size, offset, callback) {
  var self = this, readOpt = {
    size: null,
    offset: this.internalPosition || null
  };
  if(typeof size == 'function') {
    callback = size;
  } else if(typeof offset == 'function') {
    readOpt.size = size;
    callback = offset;
  } else {
    readOpt.size = size;
    readOpt.offset = offset;
  }
  new _reader(self, readOpt).end(callback);
};

Commands.prototype.readChunks = function(readOpt, callback) {
  callback(null, new _reader(this, readOpt), this._files.forRead);
};

//=========== [START] Chain : serialize class ============ //
function Chain(finish, o) {
  this.actors = new Array();
  this.o = o || {
    ignore_err: false
  };
  this.finish = finish;
};
Chain.prototype.push = function(fn) {
  this.actors.push(fn);
  return this;
};
Chain.prototype.start = function() {
  if(this.finish)
    this.actors.push(this.finish);
  this.next()(null);
};
Chain.prototype.next = function() {
  var self = this;
  return function(err) {
    if(err !== null && !self.ignore_err)
      return error(err);
    process.nextTick(execFn(Array.prototype.slice.call(arguments)));
  };
  function execFn(args) {
    return function() {
      var actor = self.actors.shift();
      actor.apply(self, args.concat(self.next()));
    };
  }
  function error(err) {
    var actor = self.actors.pop();
    if(typeof actor == "function")
      return actor(err);
    else if(err instanceof Error)
      throw err;
    else
      throw new Error(err);
  }
};
//=========== [END] Chain : serialize class ============ //

/**
 * Writes some data. This method will work properly only if initialized with
 * mode "w" or "w+".
 */
Commands.prototype.write = function(data, options, callback) {

  var self = this, EOF, finalClose;

  if(typeof options == 'function')
    callback = options, options = {};

  else if(typeof options != 'object')
    options = {};

  EOF = options['EOF'] === true ? true: false;
  finalClose = options['finalClose'] === true ? true: false;
  // TODO need pause ?
  // readStream = options['readStream'] = options['readStream'] || false;

  if(data instanceof Buffer)
    // Check if we are trying to write a buffer and use the right method
    return this.writeBuffer(data, EOF, finalClose, last);

  // Otherwise let's write the data as buffer
  this.writeBuffer(new Buffer(data, 'binary'), EOF, finalClose, last);

  function last(err) {
    callback(err, self);
  }

};

Commands.prototype.writeBuffer = function(buffer, EOF, finalClose, callback) {

  var self = this;

  if(typeof EOF == 'function')
    callback = EOF, finalClose = EOF = false

  else if(typeof finalClose == 'function')
    callback = finalClose, finalClose = false;

  // writing lock check
  this.writeEnqueue(writeBuffer);

  function writeBuffer(leftOverData) {

    if(!self.currentChunk)
      return callback(new Error(self.filename + " not opened."), null);

    if(OpenModeIs.read(self.mode))
      return callback(new Error(self.filename + " not opened for writing"),
        null);

    if(leftOverData)
      buffer = leftOverData;

    // Data exceeds current chunk remaining free size; 
    // fill up current chunk and write the rest to a new chunk (recursively)
    var currentChunkNumber = self.currentChunk.position();
    var leftOverDataSize = self.currentChunk.chunkSize
      - self.currentChunk.internalPosition;

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
    self.currentChunk.write(writeChunkData, wcb);

    function wcb(err, chunk) {

      if(err)
        return callback(err, self);

      if(leftOverData) {
        self.length += chunk.length();
        return chunk.save(leftDataSCB);
      }

      if(EOF === true) {
        self.length += chunk.length();
        return chunk.save(eofSCB);
      }

      eofSCB();

    }

    function leftDataSCB(err) {

      // TODO error handling
      if(err)
        console.error(err);

      self.currentChunk = new Chunk(self, {
        'n': ++currentChunkNumber
      });

      // call again for the remaining data in asynchrony
      // slow
      /*
      setTimeout(nextBuffer, 0);
      */
      process.nextTick(nextBuffer);

    }

    function eofSCB(err) {

      // TODO error handling
      if(err)
        console.error(err);

      // call next queue
      // slow
      /*
      setTimeout(function() {
        self.writeDequeue();
      }, 0);
      */

      process.nextTick(dequeue);

      if(finalClose === true)
        self.close();

      callback(null, self);

    }

    function nextBuffer() {

      writeBuffer(leftOverData, finalClose, callback);

    }

    function dequeue() {

      self.writeDequeue();

    }

  }

};

/**
 * Lock is accidentally remains in db when db process forcely stopped. When the
 * remains data is detected, (at waitProcess() or eventLock()) this funtion will
 * be called.
 */
// [operation]
// update status timer, delete files which flag ne 0, also delete 
// releated chunks, and clear status.
Commands.prototype.recover = function(ns_write_id, callback) {

  var self = this, tmp_val = {}, now = new Date();

  if(ns_write_id === false)
    return execRandomTimeRetryAfterRecover();

  tmp_val[Action.WRITE + '.' + ns_write_id] = now.getTime();
  tmp_val.update = now;

  this.upsetStatsData({
    _id: this.filename
  }, tmp_val, false, forceDeleteFiles);

  function forceDeleteFiles(err) {
    // TODO handle err
    self.deleteFiles({
      filename: self.filename,
      _writing: {
        $ne: 0
      }
    }, true, forceUpdateConditions);
  }

  function forceUpdateConditions(err) {
    // TODO handle err
    var tmp_val = {};
    tmp_val[Action.READ] = {}, tmp_val[Action.WRITE] = {};
    tmp_val._writing = StatIs.OPEN;
    self.upsetStatsData({
      _id: self.filename
    }, tmp_val, false, execRandomTimeRetryAfterRecover);
  }

  function execRandomTimeRetryAfterRecover(err) {
    // TODO handle err
    var time = parseInt(Math.random() * Commands.STAT_RETRY_MAX_MILLISECOND);
    OutMessage.log('recover', 'Recovered file datas of: ' + self.gs.gsid + "; "
      + self.filename + ' (retry after ' + time + ' ms, ns_write_id = '
      + ns_write_id + ')');
    // random retry for distributed environment
    return setTimeout(callback, time);
  }

};

/**
 * Lock method for read-committed environment. In writing a new file, old file
 * is accessible.
 */

//var __l = {
//  lock: {},
//  unlock: {}
//};
Commands.prototype.eventLock = function(key, lockId, statOverwrite, callback) {

  var self = this;
  var el = 'eventLock', el_s = el + '.stats';
  var val = null, res = null, no = null, now = null, now_stat = null, try_sel = null;

  if(lockId != Const.NewLock
    && !(lockId instanceof this.db.bson_serializer.ObjectID)) {
    // fatal error
    return this.errHandler(
      new Error(Err.lockUnexp('lock id not correct.', el)), callback);
  }

  if(typeof statOverwrite == 'function')
    callback = statOverwrite, statOverwrite = key == Action.READ;

  self.gs.statBegin(getStatus);

  function getStatus() {
    self.getStatsData(setStatus);
  }

  function setStatus(err, stat) {

    if(err || !stat)
      return self.errHandler(Err.statFind('', el_s), err, last);

    if(statOverwrite === false && Object.keys(stat[Action.WRITE]).length
      && self._latest[Action.WRITE]) { // file is writing now for myself

      // kick next stat waiter and push to WRITE queue.
      self.gs.statEnd(), self.gs.waitForWrite(waitTimer);
      return;

    }

    function waitTimer() {

      var time = parseInt(Math.random() * Commands.STAT_RETRY_MAX_MILLISECOND);
      OutMessage.log(el, 'waitForWrite of: ' + self.gs.gsid + "\n"
        + self.filename + ' (retry after ' + time + ' ms)');

      // should get lock again
      setTimeout(function() {
        self.gs.statBegin(getStatus);
      }, time);

    }

    now = new Date();

    val = {
      update: now,
      _writing: StatIs.LOCKED
    };

    now_stat = stat;

    // see below (try_sel = sel)
    if(try_sel != null) {
      // TODO remove 
      util.log('[gridfs:eventLock] '
        + '============ Let\'s check try_sel: ============');
      console.log('TRY-TO: ' + JSON.stringify(try_sel));
      console.log('NOW-IS: ' + JSON.stringify(stat));
      if((try_sel._no - stat._no == 0 || try_sel._no - stat._no == -1)
        && stat.update.getTime() == try_sel.update.getTime()
        && stat._writing == try_sel._writing)
        return val = stat, upsetcb(false);
      return upsetcb();
    }

    try_sel = null, no = stat._no + 1;

    var _l = self._latest[key] = {
      // TODO to function for making _id value
      _id: lockId.toString() + '-' + no,
      time: now.getTime()
    };

    val[key] = stat[key];
    val[key][_l._id] = _l.time;

    self.upsetStatsData({
      _id: self.filename,
      _writing: StatIs.OPEN
    }, val, true, upsetcb);

  }

  function upsetcb(err, n) {

    if(err)
      return self.errHandler(Err.colUpdate('', el_s), err, last);

    if(err === false && try_sel != null)
      return lockEnd();

    var sel = {
      _id: self.filename,
      _no: no
    };
    ['_writing', 'update'].forEach(function(k) {
      sel[k] = val[k];
    });

    self.existStatsData(sel, function(err, data) {
      // TODO handle error
      res = data, (data ? lockEnd: progressCheckAndRetry)();
    });

    function lockEnd() {
      self.locktime = new Date(), callback(null, res, release);
    }

    function progressCheckAndRetry() {

      // TODO remove 
      util.log('[gridfs:eventLock] '
        + '============ Let\'s progressCheckAndRetry: ============');
      console.log('SELECT: ' + JSON.stringify(sel));
      console.log('VALSET: ' + JSON.stringify(val));

      if(!now_stat)
        return self.errHandler(Err.colUpdate('', el_s), new Error(
          '[gridfs:EventLock] Unexpected no condition data.'), last);

      var ns_write = now_stat[Action.WRITE];
      var ns_write_keys = Object.keys(ns_write);
      // TODO remove 
      console.log('NS_WRITE KEYS: ' + ns_write_keys);

      var ns_write_id = ns_write_keys[0];
      var ns_write_time = ns_write[ns_write_id];
      if(try_sel == null
        && (ns_write_time == null || Date.now() - ns_write_time < Commands.STAT_FORCE_UPDATE_LIMIT_MILLISECOND)) {
        // if insert refrect failure, then _writing of criteria eq 1
        // TODO remove this bug
        try_sel = sel;
        return self.recover(false, getStatus);
      }

      try_sel = null;

      if(ns_write_keys.indexOf('recoverlock') != -1)
        // here comes after another recovering
        return self.recover(false, getStatus);

      // force update
      self.recover(ns_write_id || 'recoverlock', getStatus);

    }

  }

  function release(r_callback) {
    self.upsetStatsData({
      _id: self.filename
    }, {
      _writing: StatIs.OPEN
    }, no & 0xffffff, function(err, info) {

      if(err) {
        //fatal error
        util.error(Err.lockRelFail('', el));
        return self.errHandler(err, r_callback); //stop
      }

      //      console.log('[Lock] Released No.' + no + '/' + self.filename);
      dequeue(), r_callback(err, info);

    });
  }

  function dequeue() {

    //    util.log('[Stats Locks - eventLock::dequeue()] when delete: ' + no);
    //    util.log(':' + self.filename);
    //    console.log('lock:');
    //    console.log(JSON.stringify(__l.lock));
    //    console.log('Unlock:');
    //    console.log(JSON.stringify(__l.unlock));
    //    delete __l.lock[no];

    self.gs.statEnd();

  }

  function last(err, info) {
    dequeue(), callback(err, info);
  }

};

Commands.prototype.eventUnlock = function(callback) {

  // CLC
  // console.log('eventUnlock ' + this.gs._openNum);  

  var self = this;
  var el = 'eventUnlock', el_s = el + '.stats';
  var _la = this._latest[OpenModeIs.read(this.mode) ? Action.READ: Action.WRITE];

  if(!_la) {
    // fatal error
    this._latest = {};
    return this.errHandler(Err.lockUnexp('', el), callback);
  }

  var val = null, try_sel = null, no = null, is_mine = {};

  self.gs.statBegin(getStatus);

  function getStatus() {
    self.getStatsData(setStatus);
  }

  function setStatus(err, stat) {

    if(err || !stat)
      return self.errHandler(Err.statFind('', el_s), err, last);

    var now = new Date(), _l = self._latest;

    val = {
      update: now,
      _writing: StatIs.LOCKED
    };

    if(try_sel != null
      && (try_sel._no - stat._no == 0 || try_sel._no - stat._no == -1)
      && stat.update.getTime() == try_sel.update.getTime()
      && stat._writing == try_sel._writing)
      return val = stat, upsetcb();

    try_sel = null, no = stat._no + 1;

    [Action.READ, Action.WRITE].forEach(function(action) {

      is_mine[action] = true, val[action] = stat[action];

      if(_l[action]) {

        if(Object.keys(stat[action]).length != 1)
          is_mine[action] = false;

        delete stat[action][_l[action]._id];

      } else {

        if(Object.keys(stat[action]).length != 0)
          return is_mine[action] = false;

      }

    });

    self.upsetStatsData({
      _id: self.filename,
      _writing: StatIs.OPEN
    }, val, true, upsetcb);

  }

  function upsetcb(err, n) {

    if(err)
      return self.errHandler(Err.colUpdate('', el_s), err, last);

    if(try_sel != null)
      return lockEnd();

    val._id = self.filename, val._no = no;

    var sel = {};
    ['_id', '_no', '_writing', 'update'].forEach(function(k) {
      sel[k] = val[k];
    });

    self.existStatsData(sel,
      function(err, exist) {

        if(!exist) {

          var time = parseInt(Math.random()
            * Commands.STAT_RETRY_MAX_MILLISECOND);

          util.log('[gridfs::eventUnlock()] missing update stats of: '
            + self.gs.gsid + "\n" + self.filename + ' (retry after ' + time
            + ' ms)');

          console.log(JSON.stringify(sel));
          console.log(JSON.stringify(val));

          //        debugger;

          // if insert refrect failure, then _writing of criteria eq 1
          try_sel = sel;

          // random retry for distributed environment
          return setTimeout(getStatus, time);

        }

        //      __l.unlock[no] = self.filename;

        lockEnd();

      });

  }

  function lockEnd() {

    var both_mine = true;

    for( var action in is_mine)
      if(!is_mine[action])
        both_mine = false;

    callback(null, both_mine, release);

  }

  function release(r_callback) {

    self.upsetStatsData({
      _id: self.filename
    }, {
      _writing: StatIs.OPEN
    }, no & 0xffffff, function(err, info) {

      if(err) {

        //fatal error
        util.error(Err.lockRelFail('', el));
        return self.errHandler(err, r_callback); //stop

      }

      //      console.log('[Unlock] Released No.' + no + '/' + self.filename);

      delete self.locktime, dequeue(), r_callback(err, info);

    });

  }

  function dequeue() {

    //    util.log('[Stats Locks - eventUnlock::dequeue()] when delete: ' + no);
    //    util.log(':' + self.filename);
    //    console.log('lock:');
    //    console.log(JSON.stringify(__l.lock));
    //    console.log('Unlock:');
    //    console.log(JSON.stringify(__l.unlock));
    //    delete __l.unlock[no];

    // kick next stat user
    self.gs.statEnd();

    if(is_mine[Action.WRITE])
      self.gs.informWriteUnlock();

  }

  function last(err, info) {
    dequeue(), callback(err, info);
  }

};

Commands.prototype.setMongoObjectToSelf = function(monObj) {

  // file is found
  this.filesId = monObj._id;
  this.filename = monObj.filename;
  this.internalChunkSize = monObj.chunkSize;
  this.uploadDate = monObj.uploadDate;
  this.length = monObj.length || 0;
  this.internalMd5 = monObj.md5;

  // not in ./mongofiles put object
  this.contentType = monObj.contentType;

  // TODO check : not in monObj.
  this.aliases = monObj.aliases;
  this.metadata = monObj.metadata;

};

Commands.prototype.buildMongoObject = function(writing, callback) {

  if(typeof writing == 'function')
    callback = writing, writing = FileIs.STABLE;

  var self = this, mongoObject = {
    '_id': this.filesId instanceof this.db.bson_serializer.ObjectID
      ? this.filesId: new this.db.bson_serializer.ObjectID(),
    'filename': this.filename,
    'length': typeof this.length == 'number' ? this.length: null,
    'chunkSize': this.internalChunkSize || this.defaultChunkSize,
    'uploadDate': this.uploadDate || new Date(),
    'contentType': this.contentType,
    'aliases': this.gs.aliases || this.aliases,
    'metadata': this.gs.metadata || this.metadata,
    '_writing': writing
  };

  if(this.currentChunk && this.currentChunk.chunkSize)
    mongoObject['chunkSize'] = this.currentChunk.chunkSize;

  var md5Command = {
    filemd5: this.filesId,
    root: this.root
  };

  this.db.command(md5Command, function(err, results) {

    if(err == null && results)
      mongoObject.md5 = results.md5;

    self.setMongoObjectToSelf(mongoObject);
    callback(mongoObject);

  });

};

Commands.prototype.close = function(callback) {

  var self = this;

  if(!this.locktime) // before lock
    return self.gs.close(callback);

  // regular operation
  this.eventUnlock(function(err, is_mine, release) {

    if(err)
      return self.errHandler('EventUnlock Failure.', err, last);

    if(!is_mine)
      return last();

    var read = self._files.forRead, write = self._files.forWrite;
    var wait_to_stable = false, wait_update = false, wait_delete = false;

    // check files stats and operation
    self.filesCollection(function(err, col) {

      col.find(self.default_selector(null, REFERENCE_BY.NAME), function(err,
        cur) {

        cur.each(function(err, doc) {

          if(err || !doc)
            return nextOperation(err);

          if(wait_delete === true)
            return;

          if(doc._writing == FileIs.BEFORE_DELETE)
            return wait_delete = true;

          if(doc._writing == FileIs.ON_UPDATE)
            return write = write || doc, wait_update = true;

          if(doc._writing == FileIs.BEFORE_STABLE)
            return wait_to_stable = true;

        });

      });

    });

    function nextOperation(err) {

      if(err)
        return self.errHandler(err, last);

      if(wait_delete)
        return self.deleteFiles((read || write).filename, last);

      if(wait_to_stable)
        return last();

      if(wait_update)
        return toBeforeStable();

      last();

    }

    function toBeforeStable() {

      self.upsetFilesData(write._id, {
        length: self.length,
        _writing: FileIs.BEFORE_STABLE
      }, deleteOld);

    }

    function deleteOld() {

      if(read) {

        //        if(/file-io\/index.js/.test(self.filename))
        //          util.log('[' + self.filesId + '] ========== DELETE ========= '
        //            + read._id);
        return self.deleteFiles(read._id, true, toStable);

      }

      toStable();

    }

    function toStable() {

      //      if(/file-io\/index.js/.test(self.filename))
      //        util.log('[' + self.filesId + '] =========== DEL FINISHED ========='
      //          + (read ? read._id: 'null') + '========= TO =========' + write._id);

      self.upsetFilesData(write._id, {

        _writing: FileIs.STABLE

      }, last);

    }

    function last(err) {

      release(function(e) {

        // fatal error
        if(e) {

          util.error(Err.lockRelFail('file:' + self.filename, 'close'));
          return self.errHandler(e, callback);

        }

        self.gs.close(function(e) {

          // fatal error
          if(e)
            return self.gs.error(e, callback);

          callback(err);

        });

      });

      // CLC >>
      /*
      var cons = self.gs.namespace().Connections;
      delete cons[self.gs._openNum];
      console.log('gridstore:' + self.gs._openNum + ' close.(' + Object.keys(cons)
          + ' left.)');
      */
      // <<
      // connection close
    }

  });

};

/**
 * Gets the nth chunk of this file.
 * 
 * @param chunkNumber
 *          {number} The nth chunk to retrieve.
 * @param callback
 *          {function(*, Chunk|object)} This will be called after executing this
 *          method. null will be passed to the first parameter while a new
 *          {@link Chunk} instance will be passed to the second parameter if the
 *          chunk was found or an empty object {} if not.
 * 
 */
Commands.prototype.countChunks = function(callback) {

  var self = this;

  this.chunksCollection(function(err, col) {
    col.count({
      'files_id': self.filesId
    }, countcb);
  });

  function countcb(err, cnt) {
    callback(err, cnt);
  }

};

Commands.prototype.nthChunk = function(chunkNumber, callback) {

  var self = this;

  this.chunksCollection(function(err, col) {

    col.findOne({
      'files_id': self.filesId,
      'n': chunkNumber
    }, findcb);

  });

  function findcb(err, chunk) {
    callback(err, new Chunk(self, chunk == null ? {}: chunk));
  };

};

Commands.prototype.lastChunk = function(callback) {

  var self = this;

  this.countChunks(counted);

  function counted(err, cnt) {
    self.nthChunk(--cnt, callback);
  }

};

Commands.prototype.stat = function(callback) {

  var self = this;

  this.filesCollection(fcolcb);

  function fcolcb(err, fcol) {
    fcol.find(self.default_selector([FileIs.STABLE, FileIs.BEFORE_STABLE]), {},
      {
        slaveOk: true
      }, findcb);
  }

  function findcb(err, cur) {

    var ret = {}, doc = null;

    cur.each(function(err, data) {

      if(err || !data) {

        if(doc && doc.metadata)
          ret = doc.metadata;

        return callback(err, ret, self);

      }

      if(data._writing == FileIs.BEFORE_STABLE
        || data._writing == FileIs.STABLE && !doc)
        doc = data;

    });

  }

};

/**
 * The only thing that this function do, make files data with _writing = 3 (
 * means, FileIs.BEFORE_DELETE)
 * 
 */
Commands.prototype.deleteFile = function(callback) {

  var self = this;

  if(this.filesId == null)
    return callback(new Error('No resource to delete'));

  var id = this.filesId;
  this.filesId = null, this.filesCollection(function(err, col) {

    if(err != null)
      return delcb(err);

    self.buildMongoObject(FileIs.BEFORE_DELETE, function(monObj) {
      col.insert(monObj, delcb);
    });

  });

  function delcb(err) {
    self.filesId = id, callback(err, self);
  }

};

/**
 * Deletes the files of this file in the database.
 * 
 * @returns err (Error), res (Array): deleted files datas to delete others
 */
Commands.prototype.deleteFiles = function(key, chunks_del, callback) {

  var self = this;
  var col = null, sel = {}, cnt = null;
  var rerr = null, res = [];

  // arguments has 3 patterns
  if(typeof key == 'function')
    callback = key, chunks_del = false, key = this.filesId;
  else if(typeof chunks_del == 'function')
    callback = chunks_del, chunks_del = false;

  // key has 3 patterns acceptable.
  if(key instanceof this.db.bson_serializer.ObjectID)
    sel._id = key;
  else if(typeof key == 'string')
    sel.filename = key;
  else
    // selector
    sel = key;

  this.filesCollection(function(err, filesCollection) {

    if(err)
      delcb(err);

    col = filesCollection;
    col.find(sel, findcb);

  });

  function findcb(err, cur) {

    cur.toArray(function(err, arr) {

      if(err)
        return delcb(err);

      cnt = arr.length;
      cnt ? arr.forEach(forEachData): delcb();

      function forEachData(data) {
        var _id = data._id;
        col.remove({
          _id: _id
        }, {
          safe: true
        }, function(err) {
          // if error occurs, set as return error.
          if(err != null)
            return checkLast(err);
          chunks_del === true ? self.deleteChunks(_id, checkLast): checkLast();
        });
        function checkLast(err) {
          if(err)
            rerr = err;
          res.push(data), --cnt === 0 ? delcb(rerr): false; // in progress
        }
      }

    });

  }

  function delcb(err) {
    if(err == null)
      self.rewind();
    callback(err, res);
  }

};
/**
 * Deletes all the chunks of this file in the database.
 */
Commands.prototype.deleteChunks = function(id, callback) {

  if(typeof id == 'function')
    callback = id, id = this.filesId;

  this.chunksCollection(function(err, col) {
    col.remove({
      'files_id': id
    }, {
      safe: true
    }, callback);
  });

};

/**
 * Get fs collections.
 */
/*
Commands.prototype.filesCollection = function(callback) {

  var self = this, fname = 'filesCollection', space = '.files';

  if(this['_' + fname])
    return callback(null, this['_' + fname]);

  this.db.collection(this.root + space, collcb);

  function collcb(err, collection) {

    if(err)
      return callback(err);

    callback(null, self['_' + fname] = collection);

  }

};
Commands.prototype.chunksCollection = function(callback) {

  var self = this, fname = 'chunksCollection', space = '.chunks';

  if(this['_' + fname])
    return callback(null, this['_' + fname]);

  this.db.collection(this.root + space, collcb);

  function collcb(err, collection) {

    if(err)
      return callback(err);

    callback(null, self['_' + fname] = collection);

  }

};
Commands.prototype.statsCollection = function(callback) {

  var self = this, fname = 'statsCollection', space = '.conditions';

  if(this['_' + fname])
    return callback(null, this['_' + fname]);

  this.db.collection(this.root + space, collcb);

  function collcb(err, collection) {

    if(err)
      return callback(err);

    callback(null, self['_' + fname] = collection);

  }

};
*/

for( var name in Collections) {

  (function(name, space) {

    var fname = name + 'Collection';

    Commands.prototype[fname] = function(callback) {

      var self = this;

      if(this['_' + fname])
        return callback(null, this['_' + fname]);

      this.db.collection(this.root + space, collcb);

      function collcb(err, collection) {

        if(err)
          return callback(err);

        callback(null, self['_' + fname] = collection);

      }

    };

  })(name, Collections[name]);

}

Commands.prototype.rewind = function() {

  this.length = 0, this.uploadDate = null;

};

/**
 * Stores a file from the file system to the GridFS database.
 */
Commands.prototype.chunkIn = function(options, callback) {

  var self = this, filepath = null, size = 0;

  // arguments initialize
  if(typeof options == 'function')
    callback = options, options = {};
  else if(typeof options != 'object')
    options = {};

  filepath = options.filePath || this.gs.internalFilePath;

  if(OpenModeIs.read(this.mode))
    return this.errHandler('ChunkIn requires mode "w" or "w+".', callback);

  var ee = options.emitter || new Emitter(), poser = null;
  if(typeof callback == 'function')
    ee.on('error', last).on('end', last);

  if(options.status)
    process.nextTick(function() {
      afterStat(null, options.status);
    });
  else {
    // in this case, file must be exist in  OS FileSystem
    fs.stat(filepath, afterStat);
  }

  return ee;

  function afterStat(err, stats) {
    if(err)
      return self.errHandler('stats failed ' + filepath, err, ee);
    if(!stats || stats.size == 0)
      return ee.emit('end');
    options.contents ? writeContent(): readFileAndWrite();
  }

  function writeContent() {
    var buf = typeof options.contents == 'string'
      ? new Buffer(options.contents): options.contents;
    writeBuffer(buf, true);
  }

  function readFileAndWrite() {
    (options.stream || fs.createReadStream(filepath, {
      bufferSize: self.internalChunkSize
    })).on('error', function(e) {
      self.errHandler('write failed ' + filepath, e, ee);
    }).on('data', function(buf) {
      var stream = this;
      // breathing for another request
      if((size += buf.length) >= self.defaultChunkSize)
        stream.pause(), setTimeout(function() {
          size -= self.defaultChunkSize, stream.resume();
        }, 120);
      writeBuffer(buf)
    }).on('end', function() {
      writeBuffer(new Buffer(0), true);
    }).emit('ready');
  }

  function writeBuffer(buf, is_last) {
    self.write(buf, {
      EOF: is_last
    }, function() {
      ee.emit('data', self.currentChunk.chunkNumber, buf);
      is_last && ee.emit('end');
    });
  }

  function last(err) {
    callback(err, self);
  }

};

/**
 * Save
 */
Commands.prototype.writeFile = function(options, callback) {

  // options and callback are optional.
  var self = this;

  if(typeof options == 'function')
    callback = options, options = {};

  else if(typeof options != 'object')
    options = {};

  /* automatically set in chunkIn()
  if(!options.filePath)
    options.filePath = this.gs.internalFilePath;
  */

  var ee = options.emitter || new Emitter();
  options.emitter = ee;

  if(typeof callback == 'function')
    ee.on('error', last).on('end', last);

  this.chunkIn(options);
  return ee;

  function last(err) {
    callback(err, null, self);
  }

};

/**
 * STATS UPDATE retry interval when write open.
 * 
 * @constant
 */
Commands.STAT_RETRY_MAX_MILLISECOND = 1000;

/**
 * SAVE retry interval when write open.
 * 
 * @constant
 */
Commands.DEFAULT_SAVE_RETRY_MILLISECOND = 100;

/**
 * Write retry interval when writing.
 * 
 * @constant
 */
Commands.DEFAULT_WRITE_RETRY_MILLISECOND = 100;

/**
 * STATS FORCE UPDATE limit.
 * 
 * @constant
 */
Commands.STAT_FORCE_UPDATE_LIMIT_MILLISECOND = 5000;
