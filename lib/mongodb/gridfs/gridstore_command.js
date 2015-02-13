/***/
var fs = require('fs'), util = require('util');
var Emitter = require('events').EventEmitter, Chunk = require('./chunk').Chunk;
var timer = require('../timer');

// console logging function >>
// TODO use "util.debuglog()" and node natives
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
var OutDebug = process.env.NODE_DEBUG ? function(type, fn, message) {
  OutMessage(type, fn, message);
}: Function();

OutMessage.message = function(fn, mess) {
  var prefix = '[gridstore_commands.js:' + fn + '()] ';

  mess = typeof mess == 'function' ? mess(): mess;
  Array.isArray(mess) ? mess.unshift(prefix): (mess = prefix + mess);

  return mess;
};
// <<

// constants
var Const = {
  NewLock: 'newlock',
  RecoverLock: 'recoverlock'
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

  // stats
  //  gc._stats = {
  //    begin: null,
  //    end: null
  //  };

  // files
  gc._files = {
    forRead: null,
    forWrite: null
  };

  // internal position for chunk reading / writing
  gc.internalPosition = 0;

  // self event emitter chunk writing
  gc.writing = false, gc.waiting = [];
  gc.incomingDataLength = 0;

  // memory my latest action
  gc._latest = {};

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

/**
 * 
 */
Commands.prototype.seek = function(pos) {
  this.internalPosition = pos;
};

/**
 * 
 */
Commands.prototype.default_selector = function(writing, ref) {
  var gc = this;

  var selector = {}, id = null, name = null;

  if(typeof ref == 'number') {

    if(ref == REFERENCE_BY.ID)
      id = this.filesId;
    else
      name = this.filename;

  } else {

    if(ref instanceof gc.ObjectID)
      id = ref;
    else if(typeof ref == 'string')
      name = ref;
    else
      id = gc.filesId, name = gc.filename;

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
  var gc = this;

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
  this.close(afterClose);

};

Commands.prototype.open = function(callback) {

  var gc = this, filesCol = null, r_stats = null, mongoObject = null;
  var r_releaseHandler = null, w_releaseHandler = null;

  var gs = gc.gs, gsid = gs.gsid;
  begin();

  function begin(err) {
    var errmsg = err && (typeof err == 'string' ? err: err.message);
    if(typeof errmsg == 'string' && !/duplicate key error/.test(errmsg)) {
      // insert failure (ex. too long key)
      // ignore "E11000 duplicate key error index".
      return gc.errHandler(Err.colOpen('', 'files'), err, callback);
    }
    gc.existStatsData({}, checkExist);
  }

  function checkExist(err, exist) {

    if(err)
      util.error(err);

    //    if(/file-io\/index.js/.test(gc.filename))
    //      util.log('Stat: ' + JSON.stringify(exist));

    exist ? gc.filesCollection(fcollcb): gc.initStatsData({

      _id: gc.filename,
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
      return gc.errHandler(Err.colOpen('', 'files'), err, callback);
    (OpenModeIs.read(gc.mode) ? readEventLock: writeEventLock)();
  }

  function readEventLock() {
    gc.eventLock(Action.READ, Const.NewLock, function(err, stats, release) {

      //      if(!stats)
      //        debugger;

      r_stats = stats, r_releaseHandler = release;

      if(err) {
        return gc.errHandler(Err.lockUnexp('Read Lock failed.', 'open'), err,
          callback);
      }

      //      if(/file-io\/index.js/.test(gc.filename))
      //        util.log('Get Read Lock Succeeded: ' + JSON.stringify(stats));

      getFilesObject(null, upsetReadLock);

    });
  }

  function getFilesObject(conds, callback) {

    // get all files data to check circumstances
    filesCol.find(gc.default_selector(conds, REFERENCE_BY.NAME), {}, {

      slaveOk: true

    }, function(err, cur) {

      var obj = null, del = false;
      cur.each(function(err, data) {

        if(err || !data) {
          cur.rewind(), cur = null;
          return gc.delete_flag = del, callback(err, obj);
        }

        switch(data._writing) {

        case FileIs.STABLE:
          return !del && !obj ? obj = data: '';

        case FileIs.BEFORE_STABLE:
          return !del ? obj = data: '';

        case FileIs.ON_UPDATE:
          return;

        case FileIs.BEFORE_DELETE:
          return del = true, obj = null;

        }

        // DON'T COME HERE!
        cur.rewind(), cur = null;
        throw new Error('Unexpected _writing state: ' + data._writing);

      });

    });

  }

  function upsetReadLock(err, obj) {

    if(err)
      return gc.errHandler(Err.colFind('', 'files'), err, callback);

    // TODO need "|| {}" ?
    var sets = {}, _la = gc._latest[Action.READ] || {}, lock_key = _la._id;
    sets[Action.READ] = (r_stats || '')[Action.READ];

    if(sets[Action.READ])
      delete sets[Action.READ][lock_key];

    if(obj) {
      // TODO to function for making _id value
      _la._id = obj._id.toString() + '-' + r_stats._no.toString();
      sets[Action.READ][_la._id] = _la.time;
    } else {
      delete gc._latest[Action.READ];
      delete gc.locktime;
    }

    gc.upsetStatsData({

      _id: gc.filename,
      _writing: StatIs.LOCKED

    }, sets, r_stats._no, function() {

      r_releaseHandler(function() {
        assignAndJudge(obj);
      });

    });

  }

  function assignAndJudge(obj) {

    if(OpenModeIs.read(gc.mode) && !obj) {
      // [2014.6.24] not to leak fs.condition data
      gc.errHandler(Err.ENOENT, callback);

      timer.setTimeout(function() {
        gc.getStatsData(function(err, stats) {

          if(err || (stats || '').update != null)
            return;

          gc.removeStatsData(function() {
            // var fnam = gc.filename;
            // fnam.length > 128 && (fnam = fnam.substr(0, 128) + '...');
            // console.log('REMOVE CONDITION DATA: ', fnam);
          });

        });
      }, 800);
      return;
    }

    if(obj) // stable fs.files object
      gc._files.forRead = obj;

    if(OpenModeIs.overwrite(gc.mode) && gc.safe && obj) {
      delete gc._files.forRead;
      return gc.errHandler(Err.EEXIST, callback);
    }

    if(OpenModeIs.read(gc.mode)) {
      gc.setMongoObjectToSelf(obj);
      return gc.chunksCollection(ccolcb);
    }

    // the good end for "write" open.
    callback.call(gc);
    console.log('GOOD END FOR "WRITE" OPEN.');
    console.log(gc.filename);

  }

  function ccolcb(err, chunksCollection) {

    if(err != null)
      return gc.errHandler('filesCollection in open gridCommands.', err,
        callback);

    // (read mode) get chunks collection 
    return gc.countChunks(forReadEnd);

  }

  function forReadEnd(err, cnt) {

    /* no chunk data file exists. 
    if(err || cnt < 1)
      return gc.errHandler('No chunks data in open gridCommands.', err,
        callback);
    */

    if(err)
      return gc.errHandler('Chunks read error in open gridCommands.', err,
        callback);

    // the good end for "read" open.
    callback.call(gc);

  }

  // TODO check the mean for serverConfig.primary
  // if(/^w\+?$/.test(gc.mode) && gc.db.serverConfig.primary != null)

  function writeEventLock() {
    // set new fs.files with writing = 1.
    gc.filesId = gc.uploadDate = gc.length = null;
    if(mongoObject)
      return eventLockId(mongoObject); // NEVER CHANGE LOCK-ID!
    gc.buildMongoObject(FileIs.ON_UPDATE, eventLockId);
  }

  function eventLockId(monObj) {
    // lock on node for writing object
    gc.eventLock(Action.WRITE, monObj._id, function(err, stats, release) {

      w_releaseHandler = release, mongoObject = monObj;

      if(err)
        return gc.errHandler(Err.lockUnexp('Write Lock failed.', 'open'), err,
          callback);

      filesCol.count(gc.default_selector([FileIs.BEFORE_STABLE,
        FileIs.ON_UPDATE], REFERENCE_BY.NAME), countcb);

    });
  }

  function countcb(err, cnt) {
    if(cnt) // writing process is in progress ( is there such a situation ? )
      return w_releaseHandler(waitProcess);
    return gc.upsertFilesData(mongoObject, savedcb);
  }

  function savedcb(err) {

    w_releaseHandler(function() {
      filesCol.findOne(gc.default_selector(FileIs.ON_UPDATE), {
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
      return gc.errHandler(Err.colFind('new fs.files failure.', 'files'), err,
        callback);

    if(!obj) // save incomplete because of unique index
      return waitProcess();

    gc._files.forWrite = obj;
    (OpenModeIs.overwrite(gc.mode) ? setChunk: setCursor)();

  }

  function setCursor() {
    gc.lastChunk(setChunk);
  }

  function setChunk(err, chunk) {

    gc.length = 0;
    gc.currentChunk = chunk instanceof Chunk ? chunk: new Chunk(gc, {
      'n': 0
    });

    if(err)
      return gc.errHandler(Err.colFind('lastChunk failure.', 'files'), err,
        callback);

    // the good end for write open. ( callback after read object checking )
    getFilesObject([FileIs.STABLE], function(err, obj) {
      assignAndJudge(obj);
    });

  }

  function waitProcess() {

    // waiting log
    var title = 'Enter condition: ' + gsid + '; ' + gc.filename;
    console.log('wp1');
    OutDebug('waitProcess', title
      + ' (writingChunk stats='
      + (gc._waitingChunk ? gc._waitingChunk.obj._id + ':'
        + gc._waitingChunk.cnt: 'undefined') + ')');
    // ex: [gridfs] enter waitProcess(): (writingChunk stats=undefined:0)

    if(!gc._waitingChunk)
      gc._waitingChunk = {};

    gc.filesCollection(function(err, col) {
      console.log('wp2', err);

      col.findOne(gc.default_selector([FileIs.BEFORE_STABLE, FileIs.ON_UPDATE],
        REFERENCE_BY.NAME), {
        slaveOk: false
      }, wpFindcb);
      console.log('wp3', err);

    });

  }

  function wpFindcb(err, obj) {
    console.log('wp4', err, obj);

    if(err)
      return gc.errHandler('Find failure on waitPrcess.', err, callback);

    // if not exist neither BEFORE_STABLE nor ON_UPDATE file,
    // ,or after the major process is finished writing.
    if(!obj) {
      return endOfWait();
    }

    // when first wait, set waiting circumstances
    if(Object.keys(gc._waitingChunk).length == 0) {
      gc._waitingChunk.obj = obj;
      gc._waitingChunk.cnt = null;
    }

    // set files condition, just now.
    gc._waitingChunk.objNow = obj;

    console.log('wp5');
    // find chunks condition
    gc.chunksCollection(function(err, col) {
      console.log('wp6');
      col.count({
        'files_id': obj._id
      }, wpCountcb);
      console.log('wp6');
    });

  }

  function wpCountcb(err, cnt) {

    console.log('wp7', err, cnt);
    if(err) {
      return endOfWait(err);
    }

    if(!cnt)
      cnt = 0;

    var wc = gc._waitingChunk;
    gc.getStatsData(function(err, stats) {

      console.log('wp8', err, stats);
      if(err) {
        return endOfWait(err);
      }

      var el = stats[Action.WRITE];

      // if unexpected stats data, then throw error
      if(!el || typeof el != 'object')
        return endOfWait(new Error(OutMessage.message('waitProcess',
          'Unexpected WRITE value; ' + el)));

      OutMessage('waitProcess', 'wpCountcb: ' + gsid + '; MongoObject._id: '
        + mongoObject._id.toString());

      console.log('_waitingChunk: ', wc);
      console.log('stats[Action.WRITE]: ', el);

      // get writing files_id
      var el_id = (Object.keys(el)[0] || '').substr(0, 24);

      // writing have already finished
      if(!el_id || wc.clean === true) {
        return endOfWait();
      }

      // regular circumstances

      /* 1. first wait 
       *    (wc.cnt == null)
       * 2. on writing chunk is changed 
       *    (wc.cnt != cnt) */
      if(wc.cnt == null || wc.cnt != cnt) // 1 and 2
        return setWaitingTimeout();

      // set whether lock is mine or other process
      var is_mine_now = el_id === wc.obj._id.toString();
      if(wc.is_mine == null)
        wc.is_mine = is_mine_now;

      /* 3. writing cluster is changed (gc or other process, usually not mine)
       *    (el_id === obj._id == wc.is_mine)
       * 3-1[is_mine == false]. on writing object is changed (other proces)
       *    (obj._id != objNow._id)
       * 3-2[is_mine == true ]. on writing object is changed (gc)
       *    // rare case
       *    (el_id != objNow._id)
       * 3-3[is_mine == true ]
       */
      if(wc.is_mine != is_mine_now) // 3
        return setWaitingTimeout();

      if(wc.is_mine) { // IT'S MY TURN!

        if(el_id != wc.objNow._id.toString()) // 3-2
          setWaitingTimeout();
        else
          endOfWait(); // 3-3

        return;
      }

      if(wc.obj._id.toString() != wc.objNow._id.toString()) // 3-1
        return setWaitingTimeout();

      // exec recover with random timeout
      var timeout = Math.random()
        * Commands.STAT_FORCE_UPDATE_LIMIT_MILLISECOND;

      OutMessage('waitProcess', 'Go to recover. (after ' + timeout + ' ms)');
      console.log('gsid: ' + gsid);

      timer.setTimeout(function() {
        wc.clean = true;
        gc.recover(wc.objNow._id, setWaitingTimeout);
      }, timeout);

      function setWaitingTimeout() {

        OutMessage('waitProcess', 'setWaitingTimeout.');
        console.log('GSID     : ' + gsid);
        console.log('WritingID: ' + el_id + ', for ' + gc.filename);

        wc.cnt = cnt, wc.obj = wc.objNow;

        var timeout = Math.random() * Commands.DEFAULT_SAVE_RETRY_MILLISECOND;
        timer.setTimeout(waitProcess, timeout);

      }

    });

  }

  function endOfWait(err) {
    delete gc._waitingChunk;
    err ? fcollcb(err): gc.filesCollection(fcollcb);
  }

};

/**
 * 
 */
Commands.prototype.initStatsData = function(stats, callback) {
  var gc = this;
  gc.statsCollection(function(err, statsCollection) {

    if(err)
      return callback(err);

    statsCollection.insert(stats, callback);

  });
};

/**
 * @param callback
 */
Commands.prototype.existStatsData = function(sel, callback) {
  var gc = this;
  gc.statsCollection(function(err, statsCollection) {

    if(err)
      return callback(err);

    sel._id = gc.filename;
    statsCollection.findOne(sel, callback);

  });
};

/**
 * @param callback
 */
Commands.prototype.getStatsData = function(callback) {
  var gc = this;
  gc.statsCollection(function(err, statsCollection) {

    if(err)
      return callback(err);

    statsCollection.findOne({
      _id: gc.filename
    }, callback);

  });
};

/**
 * @param callback
 */
Commands.prototype.removeStatsData = function(callback) {
  var gc = this;
  gc.statsCollection(function(err, statsCollection) {

    if(err)
      return callback(err);

    statsCollection.remove({
      _id: gc.filename
    }, callback);

  });
};

/**
 * @param callback
 */
Commands.prototype.upsetStatsData = function(criteria, stats, no, callback) {
  var gc = this;
  gc.statsCollection(function(err, statsCollection) {

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
      safe: !!callback
    }, callback);

  });
};

/**
 * @param callback
 */
Commands.prototype.upsertFilesData = function(monObj, callback) {
  var gc = this;
  gc.filesCollection(function(err, filesCollection) {

    if(err)
      return callback(err);

    filesCollection.save(monObj, {
      safe: !!callback
    }, callback);

  });
};

/**
 * @param callback
 */
Commands.prototype.upsetFilesData = function(id, obj, callback) {
  var gc = this;
  gc.filesCollection(function(err, filesCollection) {

    if(err)
      return callback(err);

    filesCollection.update({
      _id: id
    }, {
      $set: obj
    }, {
      safe: !!callback
    }, callback);

  });
};

// =========== [START] _reader : chunk read class ============ //
/**
 * @constructor _reader
 */
function _reader(gc, options) {
  var _r = this;

  Emitter.call(_r);
  otpions = options || {}

  var size = parseInt(options.size || null);
  var offset = options.offset || 0;
  var chunk_size = gc.internalChunkSize || gc.defaultChunkSize;

  _r.gc = gc;
  _r.offset_chunk_num = parseInt(offset / chunk_size);
  _r.offset = offset - chunk_size * _r.offset_chunk_num;
  _r.size = size;
  _r.encode = gc.gs.contentType.slice(0, 4) == 'text' ? options.encode
    || 'utf8': null;
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
  var emitData = this.encode == null ? emitDataBuffer: emitDataString;
  _r.on('data', function() {

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
    _r.on(DataEvent.Error, function(e) {
      endCallback(e);
    }).on(DataEvent.Data, function(n, buf) {
      chunks = Buffer.concat([chunks, buf]);
    }).on(DateEvent.End, function(byte) {
      var r = encode == null ? chunks: chunks.toString(encode);
      endCallback.call(gc, null, byteRead, r);
    });

  // execute reading process
  gc.countChunks(countcb);

  function countcb(err, cnt) {

    if(err)
      return finish(err);

    if(cnt == 0)
      return finish(new Error('No chunk data for ' + gc.filename));

    maxChunk = cnt - 1, process.nextTick(function() {
      _nth(_r.offset_chunk_num);
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

      var over = _r.size > 0 ? read - _r.size: false;
      var fin = n >= maxChunk || (over && over >= 0);
      if(over > 0) { // read over, to finish
        read -= over;
        chunk = chunk.slice(0, chunk_len - over);
      }

      emitData(n, chunk);

      if(_r._fin != null) // if stop signal recieved
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
//=========== [END] Reader : chunk read class ============ //

Commands.prototype.read = function(size, offset, callback) {
  var gc = this;

  var readOpt = {
    size: null,
    offset: gc.internalPosition || null
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

  return new _reader(gc, readOpt).end(callback);
};

Commands.prototype.readChunks = function(readOpt, callback) {
  callback(null, new _reader(this, readOpt), this._files.forRead);
};

/**
 * Writes some data. This method will work properly only if
 * initialized with mode "w" or "w+".
 */
Commands.prototype.write = function(data, options, callback) {
  var gc = this;

  var EOF, finalClose;
  if(typeof options == 'function')
    callback = options, options = {};
  else if(typeof options != 'object')
    options = {};

  EOF = options['EOF'] === true ? true: false;
  finalClose = options['finalClose'] === true ? true: false;
  // TODO need pause ?
  // readStream = options['readStream'] = options['readStream'] || false;

  if(data instanceof Buffer) {
    // Check if we are trying to write a buffer and use the right method
    gc.writeBuffer(data, EOF, finalClose, last);
    return;
  }

  // Otherwise let's write the data as buffer
  gc.writeBuffer(new Buffer(data, 'binary'), EOF, finalClose, last);

  function last(err) {
    callback(err, gc);
  }

};

Commands.prototype.writeBuffer = function(buffer, EOF, finalClose, callback) {
  var gc = this;

  if(typeof EOF == 'function')
    callback = EOF, finalClose = EOF = false
  else if(typeof finalClose == 'function')
    callback = finalClose, finalClose = false;

  // writing lock check
  gc.writeEnqueue(writeBuffer);

  function writeBuffer(leftOverData) {

    if(!gc.currentChunk)
      return callback(new Error(gc.filename + " not opened."), null);

    if(OpenModeIs.read(gc.mode))
      return callback(new Error(gc.filename + " not opened for writing"), null);

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
    gc.currentChunk.write(writeChunkData, wcb);

    function wcb(err, chunk) {

      if(err)
        return callback(err, gc);

      if(leftOverData) {
        gc.length += chunk.length();
        return chunk.save(leftDataSCB);
      }

      if(EOF === true) {
        gc.length += chunk.length();
        return chunk.save(eofSCB);
      }

      eofSCB();

    }

    function leftDataSCB(err) {

      // TODO error handling
      if(err)
        console.error(err);

      gc.currentChunk = new Chunk(gc, {
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
        gc.writeDequeue();
      }, 0);
      */

      process.nextTick(dequeue);

      if(finalClose === true)
        gc.close();

      callback(null, gc);

    }

    function nextBuffer() {
      writeBuffer(leftOverData, finalClose, callback);
    }

    function dequeue() {
      gc.writeDequeue();
    }

  }

};
/**
 * Check the condition and kick recover logic. if only wait
 * is needed, pass "false" to recover logic.
 */
Commands.prototype.recoverCondition = function(now_stat, try_sel, callback) {
  var gc = this;

  var now = Date.now(), recoverNow = function(compare) {
    return now - compare > Commands.STAT_FORCE_UPDATE_LIMIT_MILLISECOND;
  };

  // simple random retry
  //  if(now_stat === false)
  //    return self.recover(false, callback);

  var ns_write = now_stat[Action.WRITE];
  var ns_write_keys = Object.keys(ns_write);

  // TODO remove 
  //console.log('NS_WRITE KEYS: ', ns_write_keys);

  var ns_write_id = ns_write_keys[0];
  ns_write_id == Const.RecoverLock && (ns_write_id = null);

  var ns_write_time = ns_write[ns_write_id];

  // simple failure
  if(ns_write_id == null && try_sel == null
    && (!now_stat.update || recoverNow(now_stat.update.getTime()) === false)) {
    return gc.recover(false, callback), true;
  }

  // accept more wait
  if(ns_write_id && try_sel == null
    && (ns_write_time == null || recoverNow(ns_write_time) === false)) {
    // if insert refrect failure, then _writing of criteria eq 1
    // TODO more simple?
    return gc.recover(false, callback), true;
  }

  // wait another recover
  if(ns_write_keys.indexOf(Const.RecoverLock) != -1
    && recoverNow(ns_write[Const.RecoverLock]) === false)
    // here comes after another recovering
    return gc.recover(false, callback);

  // force update
  gc.recover(ns_write_id || Const.RecoverLock, callback);
};
/**
 * Lock is accidentally remains in db when db process
 * forcely stopped. When the remains data is detected, (at
 * waitProcess() or eventLock()) this funtion will be
 * called.
 */
// [operation]
// update status timer, delete files which flag ne 0, also delete 
// releated chunks, and clear status.
Commands.prototype.recover = function(ns_write_id, callback) {

  var gc = this, tmp_val = {}, now = new Date();

  var gs = gc.gs, gsid = gs.gsid;
  if(ns_write_id === false)
    return execRandomTimeRetryAfterRecover();

  tmp_val[Action.WRITE + '.' + ns_write_id] = now.getTime();
  tmp_val.update = now;

  // real retry log
  // TODO debug
  OutMessage('recover', 'Recover file. (ns_write_id = ' + ns_write_id + ')');
  console.log('GSID: ' + gsid + ', for ' + gc.filename);

  gc.upsetStatsData({
    _id: gc.filename
  }, tmp_val, false, forceDeleteFiles);

  function forceDeleteFiles(err) {

    if(err)
      return execRandomTimeRetryAfterRecover(err)

    gc.deleteFiles({
      filename: gc.filename,
      _writing: {
        $ne: 0
      }
    }, true, forceUpdateConditions);

  }

  function forceUpdateConditions(err) {

    if(err)
      return execRandomTimeRetryAfterRecover(err)

    var tmp_val = {};
    tmp_val[Action.READ] = {}, tmp_val[Action.WRITE] = {};
    tmp_val._writing = StatIs.OPEN;

    gc.upsetStatsData({
      _id: gc.filename
    }, tmp_val, false, execRandomTimeRetryAfterRecover);

  }

  function execRandomTimeRetryAfterRecover(err) {

    if(err) {
      OutMessage('recover', 'recover error.');
      console.error(err);
    }

    // TODO handle err
    // random retry for distributed environment
    var timeout = parseInt(Math.random() * Commands.STAT_RETRY_MAX_MILLISECOND);
    return timer.setTimeout(callback, timeout);

  }

};

//var __l = {
//  lock: {},
//  unlock: {}
//};

/**
 * Lock method for read-committed environment. In writing a
 * new file, old file is accessible.
 */
Commands.prototype.eventLock = function(key, lockId, statOverwrite, callback) {

  var gc = this, gs = gc.gs, gsid = gs.gsid;
  var el = 'eventLock', el_s = el + '.stats';

  var in_queue = false;
  var val = null, res = null, no = null, now = null;
  var now_stat = null, try_sel = null;

  // invalid lock leads fatal error
  var invalid_lock = lockId != Const.NewLock
    && !(lockId instanceof gc.ObjectID);
  if(invalid_lock) {
    return gc.errHandler(new Error(Err.lockUnexp('lock id not correct.', el)),
      callback);
  }

  if(typeof statOverwrite == 'function')
    callback = statOverwrite, statOverwrite = key == Action.READ;

  // begin queueing globally (eventLock)
  gs.statBegin(getStatus);

  function getStatus() {
    gc.getStatsData(setStatus);
  }

  function setStatus(err, stat) {

    if(err || !stat)
      return gc.errHandler(Err.statFind('', el_s), err, last);

    if(statOverwrite === false && in_queue === false) { // pipeline writing mode
      //    if(statOverwrite === false && gc._latest[Action.WRITE]) {

      //      var now = Date.now(), stat_w = stat[Action.WRITE] || {};
      //      for( var i in stat_w) { // remove old values
      //        if(now > stat_w[i] + Commands.STAT_FORCE_UPDATE_LIMIT_MILLISECOND)
      //          delete stat_w[i];
      //      }

      // kick next stat waiter and push to WRITE queue.
      gs.statEnd(), gs.waitForWrite(function() {

        // in_queue flag up, and should get lock again
        in_queue = true, timer.setTimeout(function() {
          gs.statBegin(getStatus);
        }, Commands.DEFAULT_WRITE_RETRY_MILLISECOND);

      });
      return;
      //      }
    }

    // read or overwrite mode always in_queue true,
    in_queue = true;

    // pipeline execution of .upsetStat()
    upsetStat(stat);

  }

  function upsetStat(stat) {

    now = new Date();
    now_stat = stat;

    val = {
      update: now,
      _writing: StatIs.LOCKED
    };

    if(try_sel != null) {
      //util.log('[gridfs:eventLock] '
      //  + '============ Let\'s check try_sel: ============');
      //console.log('eventLock.TRY-TO: ' + JSON.stringify(try_sel));
      //console.log('eventLock.NOW-IS: ' + JSON.stringify(stat));
      if(try_sel._no - stat._no == 0
        && stat.update.getTime() == try_sel.update.getTime()
        && stat._writing == try_sel._writing)
        // update successfully but unexpected find failed.
        return res = stat, upsetcb(false);
    }

    try_sel = null, no = stat._no + 1;

    var _l = gc._latest[key] = {};
    // TODO to function for making _id value
    _l._id = lockId.toString() + '-' + no;
    _l.time = now.getTime();

    val[key] = stat[key];
    val[key][_l._id] = _l.time;

    gc.upsetStatsData({
      _id: gc.filename,
      _writing: StatIs.OPEN
    }, val, true, upsetcb);

  }

  function upsetcb(err, n) {

    if(err)
      return gc.errHandler(Err.colUpdate('', el_s), err, last);

    if(err === false && try_sel != null)
      return locked();

    var sel = {
      _id: gc.filename,
      _no: no
    };
    ['_writing', 'update'].forEach(function(k) {
      sel[k] = val[k];
    });

    gc.existStatsData(sel, function(err, stat) {
      // TODO handle error?
      res = stat, (stat ? locked: progressCheckAndRetry)();
    });

    function progressCheckAndRetry() {
      if(!now_stat)
        return gc.errHandler(Err.colUpdate('', el_s), new Error(
          '[gridfs:eventLock] Unexpected no condition data.'), last);
      try_sel = gc.recoverCondition(now_stat, try_sel, getStatus) === true
        ? sel: null;
    }

  }

  // callback after [eventLock] succeeded.
  function locked() {
    gc.locktime = new Date(), callback(null, res, release);
  }

  // this function will call when error occurs on locking.
  function last(err, info) {
    statDequeue(), callback(err, info);
  }

  // this function will pass to callback.
  function release(r_callback) {
    gc.upsetStatsData({
      _id: gc.filename
    }, {
      _writing: StatIs.OPEN
    }, no & 0xffffff, function(err, info) {

      // always dequeue to next
      statDequeue();

      if(err) {
        // fatal error
        util.error(Err.lockRelFail('', el));
        return gc.errHandler(err, r_callback); //stop
      }

      // console.log('[Lock] Released No.' + no + '/' + gc.filename);
      r_callback(err, info);

    });
  }

  function statDequeue() { // for eventLock()

    //    util.log('[Stats Locks - eventLock::statDequeue()] when delete: ' + no);
    //    util.log(':' + gc.filename);
    //    console.log('lock:');
    //    console.log(JSON.stringify(__l.lock));
    //    console.log('Unlock:');
    //    console.log(JSON.stringify(__l.unlock));
    //    delete __l.lock[no];

    // kick next stat user
    gs.statEnd();

  }

};

Commands.prototype.eventUnlock = function(callback) {

  // CLC
  // console.log('eventUnlock ' + this.gs._openNum);  

  var gc = this, gs = gc.gs, gsid = gs.gsid;
  var el = 'eventUnlock', el_s = el + '.stats';
  var _la = gc._latest[OpenModeIs.read(gc.mode) ? Action.READ: Action.WRITE];

  if(!_la) {
    // fatal error before queueing
    gc._latest = {};
    return gc.errHandler(Err.lockUnexp('', el), callback);
  }

  var val = null, no = null, is_mine = {};
  var try_sel = null, now_stat = null;

  // begin queueing globally (eventUnlock)
  gs.statBegin(getStatus);

  function getStatus() {
    gc.getStatsData(setStatus);
  }

  function setStatus(err, stat) {

    if(err || !stat)
      return gc.errHandler(Err.statFind('', el_s), err, last);

    var now = new Date(), _l = gc._latest;

    val = {
      update: now,
      _writing: StatIs.LOCKED
    };

    now_stat = stat;

    if(try_sel != null) {
      //util.log('[gridfs:eventUnlock] '
      //  + '============ Let\'s check try_sel: ============');
      //console.log('eventUnlock.TRY-TO: ' + JSON.stringify(try_sel));
      //console.log('eventUnlock.NOW-IS: ' + JSON.stringify(stat));
      if(try_sel._no - stat._no == 0
        && stat.update.getTime() == try_sel.update.getTime()
        && stat._writing == try_sel._writing)
        // update successfully but unexpected find failed.
        return val = stat, upsetcb(false);
    }

    try_sel = null, no = stat._no + 1;
    [Action.READ, Action.WRITE].forEach(function(act) {

      // copy value
      val[act] = stat[act];
      if((is_mine[act] = !!_l[act]) === false)
        return;

      var keys = Object.keys(stat[act]);
      if(keys.length != 1) {
        is_mine[act] = false;
        //console.log('!!!!IS NOT MY TREAT!!!!!', act, keys, _l[act]);
      }
      delete val[act][_l[act]._id];

    });

    gc.upsetStatsData({
      _id: gc.filename,
      _writing: StatIs.OPEN
    }, val, true, upsetcb);

  }

  function upsetcb(err, n) {

    if(err)
      return gc.errHandler(Err.colUpdate('', el_s), err, last);

    if(err === false && try_sel != null)
      return locked();

    val._id = gc.filename, val._no = no;

    var sel = {};
    ['_id', '_no', '_writing', 'update'].forEach(function(k) {
      sel[k] = val[k];
    });

    gc.existStatsData(sel, function(err, data) {
      // TODO handle error?
      res = data, (data ? locked: progressCheckAndRetry)(sel);
    });

  }

  function progressCheckAndRetry(sel) {
    if(!now_stat)
      return gc.errHandler(Err.colUpdate('', el_s), new Error(
        '[gridfs:eventUnlock] Unexpected no condition data.'), last);
    try_sel = gc.recoverCondition(now_stat, try_sel, getStatus) === true ? sel
      : null;
  }

  // callback after [eventUnlock] succeeded.
  function locked() {
    callback(null, is_mine[Action.WRITE] || !is_mine[Action.READ], release);
  }

  // this function will call when error occurs on locking.
  function last(err, info) {
    statDequeue(), callback(err, info);
  }

  // this function will pass to callback.
  function release(r_callback) {

    gc.upsetStatsData({
      _id: gc.filename
    }, {
      _writing: StatIs.OPEN
    }, no & 0xffffff, function(err, info) {
      delete gc.locktime, statDequeue()

      if(err) {

        //fatal error
        util.error(Err.lockRelFail('', el));
        return gc.errHandler(err, r_callback); //stop

      }

      //      console.log('[Unlock] Released No.' + no + '/' + gc.filename);
      r_callback(err, info);
    });

  }

  function statDequeue() { // for eventUnlock()

    //    util.log('[Stats Locks - eventUnlock::statDequeue()] when delete: ' + no);
    //    util.log(':' + gc.filename);
    //    console.log('lock:');
    //    console.log(JSON.stringify(__l.lock));
    //    console.log('Unlock:');
    //    console.log(JSON.stringify(__l.unlock));
    //    delete __l.unlock[no];

    // kick next stat user
    gs.statEnd();

    if(is_mine[Action.WRITE])
      gs.informWriteUnlock();
  }

};

Commands.prototype.setMongoObjectToSelf = function(monObj) {
  var gc = this;

  // file is found
  gc.filesId = monObj._id;
  gc.filename = monObj.filename;
  gc.internalChunkSize = monObj.chunkSize;
  gc.uploadDate = monObj.uploadDate;
  gc.length = monObj.length || 0;
  gc.internalMd5 = monObj.md5;

  // not in ./mongofiles put object
  gc.contentType = monObj.contentType;

  // TODO check : not in monObj.
  gc.aliases = monObj.aliases;
  gc.metadata = monObj.metadata;

};

Commands.prototype.buildMongoObject = function(writing, callback) {
  var gc = this;

  if(typeof writing == 'function')
    callback = writing, writing = FileIs.STABLE;

  var is_object_id = gc.filesId instanceof gc.ObjectID;
  var mongoObject = {
    '_id': is_object_id ? gc.filesId: new gc.ObjectID(),
    'filename': gc.filename,
    'length': typeof gc.length == 'number' ? gc.length: null,
    'chunkSize': gc.internalChunkSize || gc.defaultChunkSize,
    'uploadDate': gc.uploadDate || new Date(),
    'contentType': gc.contentType,
    'aliases': gc.gs.aliases || gc.aliases,
    'metadata': gc.gs.metadata || gc.metadata,
    '_writing': writing
  };

  if(gc.currentChunk && gc.currentChunk.chunkSize)
    mongoObject['chunkSize'] = gc.currentChunk.chunkSize;

  var md5Command = {
    filemd5: gc.filesId,
    root: gc.root
  };

  gc.db.command(md5Command, function(err, results) {

    if(err == null && results)
      mongoObject.md5 = results.md5;

    gc.setMongoObjectToSelf(mongoObject);
    callback(mongoObject);

  });

};

Commands.prototype.close = function(callback) {

  var gc = this, gs = gc.gs, gsid = gs.gsid;
  if(!gc.locktime) // before lock
    return gs.close(closeCallback);

  // debug
  if(OpenModeIs.write(gc.mode)) {
    util.log('GOTO CLOSE.', gs.gsid);
    console.log(gc.filename);
  }

  // regular operation
  gc.eventUnlock(function(err, is_mine, release) {

    if(OpenModeIs.write(gc.mode)) {
      console.log('Unlock succeed.', err);
      console.log(gc.filename);
    }

    if(err)
      return gc.errHandler('EventUnlock Failure.', err, last);

    if(!is_mine)
      return last();

    var read = gc._files.forRead, write = gc._files.forWrite;
    var wait_to_stable = false, wait_update = false, wait_delete = false;

    // check files stats and operation
    gc.filesCollection(function(err, col) {

      col.find(gc.default_selector(null, REFERENCE_BY.NAME),
        function(err, cur) {
          cur.each(function(err, doc) {

            if(err || !doc) {
              cur.rewind(), cur = null;
              return nextOperation(err);
            }

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

      if(OpenModeIs.write(gc.mode)) {
        console.log('nextOperation.', err);
        console.log(gc.filename);
      }

      if(err)
        return gc.errHandler(err, last);

      if(wait_delete)
        return gc.deleteFiles((read || write).filename, last);

      if(wait_to_stable)
        return last();

      if(wait_update)
        return toBeforeStable();

      last();

    }

    function toBeforeStable() {

      gc.upsetFilesData(write._id, {
        length: gc.length,
        _writing: FileIs.BEFORE_STABLE
      }, deleteOld);

    }

    function deleteOld() {

      if(read) {
        //        if(/file-io\/index.js/.test(gc.filename))
        //          util.log('[' + gc.filesId + '] ========== DELETE ========= '
        //            + read._id);
        return gc.deleteFiles(read._id, true, toStable);
      }

      toStable();

    }

    function toStable() {

      //      if(/file-io\/index.js/.test(gc.filename))
      //        util.log('[' + gc.filesId + '] =========== DEL FINISHED ========='
      //          + (read ? read._id: 'null') + '========= TO =========' + write._id);

      gc.upsetFilesData(write._id, {
        _writing: FileIs.STABLE
      }, last);

    }

    function last(err) {

      if(OpenModeIs.write(gc.mode)) {
        console.log('last.', err);
        console.log(gc.filename);
      }

      release(function(e) {
        if(OpenModeIs.write(gc.mode)) {
          console.log('released.', err);
          console.log(gc.filename);
        }
        // fatal error
        if(e) {
          util.error(Err.lockRelFail('file:' + gc.filename, 'close'));
          return gc.errHandler(e, callback);
        }
        gs.close(closeCallback);
      }); // << release(function(){})
      // CLC >>
      /*
      var cons = gc.gs.namespace().Connections;
      delete cons[gc.gs._openNum];
      console.log('gridstore:' + gc.gs._openNum + ' close.(' + Object.keys(cons)
          + ' left.)');
      */
      // <<
      // connection close
    }
  });
  function closeCallback(e) {

    if(OpenModeIs.write(gc.mode)) {
      util.log('GOTO closeCallback.', gs.gsid);
      console.log(gc.filename);
    }

    // fatal error
    if(e) {
      gs.error(e, callback);
    } else {
      callback(null);
    }

    // memory leak care
    setImmediate(function() {

      // release targets
      var _ = [];
      _.push.apply(_, CollectionsList);
      _.push.apply(_, ['_files', '_waitingChunk', '_latest']);
      _.push.apply(_, ['gs', 'db', 'ObjectID']);

      // release
      _.forEach(function(k) {
        delete gc[k];
      });
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
  gc.chunksCollection(function(err, col) {

    if(err)
      return callback(err);

    col.count({
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

  function counted(err, cnt) {
    gc.nthChunk(--cnt, callback);
  }

};

Commands.prototype.stat = function(callback) {

  var gc = this;
  gc.filesCollection(fcolcb);

  function fcolcb(err, fcol) {
    fcol.find(gc.default_selector([FileIs.STABLE, FileIs.BEFORE_STABLE]), {}, {
      slaveOk: true
    }, findcb);
  }

  function findcb(err, cur) {
    var doc = '';
    cur.each(function(err, data) {

      if(err || !data) {
        cur.rewind(), cur = null;
        return callback(err, doc.metadata || {}, gc);
      }

      if(data._writing == FileIs.BEFORE_STABLE
        || data._writing == FileIs.STABLE && !doc)
        doc = data;

    });
  }

};

/**
 * The only thing that this function do, make files data
 * with _writing = 3 ( means, FileIs.BEFORE_DELETE)
 * 
 */
Commands.prototype.deleteFile = function(callback) {

  var gc = this;

  if(gc.filesId == null)
    return callback(new Error('No resource to delete'));

  var id = gc.filesId;
  gc.filesId = null, gc.filesCollection(function(err, col) {

    if(err != null)
      return delcb(err);

    gc.buildMongoObject(FileIs.BEFORE_DELETE, function(monObj) {
      col.insert(monObj, delcb);
    });

  });

  function delcb(err) {
    gc.filesId = id, callback(err, gc);
  }

};

/**
 * Deletes the files of this file in the database.
 * 
 * @returns err (Error), res (Array): deleted files datas to
 *          delete others
 */
Commands.prototype.deleteFiles = function(key, chunks_del, callback) {
  var gc = this;

  var col = null, sel = {}, cnt = null;
  var rerr = null, res = [];

  // arguments has 3 patterns
  if(typeof key == 'function')
    callback = key, chunks_del = false, key = gc.filesId;
  else if(typeof chunks_del == 'function')
    callback = chunks_del, chunks_del = false;

  // key has 3 patterns acceptable.
  if(key instanceof gc.ObjectID)
    sel._id = key;
  else if(typeof key == 'string')
    sel.filename = key;
  else
    // selector
    sel = key;

  gc.filesCollection(function(err, filesCollection) {

    if(err)
      return delcb(err);

    col = filesCollection;
    col.find(sel, findcb);

  });

  function findcb(err, cur) {

    if(err) {
      cur = null;
      return delcb(err);
    }

    var prms = [];
    cur.each(function(err, doc) {

      if(err) {
        cur.rewind(), cur = null;
        return delcb(err);
      }

      if(!doc) {
        cur.rewind(), cur = null;
        return Promise.all(prms).then(function() {
          delcb(); // DON'T CALL DIRECT. PROMISE.ALL RETURNS "[]"
        })['catch'](delcb);
      }

      var _id = doc._id;
      prms.push(new Promise(function(rsl, rej) {
        col.remove({
          _id: _id
        }, {
          safe: true
        }, function(err) {

          // if error occurs, set as return error.
          if(err) {
            return rej(err);
          }

          gc.deleteChunks(_id, function(err) {
            err ? rej(err): rsl();
          });

        });
      }));

    });
  } // << findcb()

  function delcb(err) {
    err == null && gc.rewind(), callback(err);
  } // << delcb()

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
} // << each collection getter prototypes

Commands.prototype.rewind = function() {
  this.length = 0, this.uploadDate = null;
};

/**
 * Stores a file from the file system to the GridFS
 * database.
 */
Commands.prototype.chunkIn = function(options, callback) {
  var gc = this, gs = gc.gs;

  // local variables
  var filepath = null, size = 0;

  // arguments initialize
  if(typeof options == 'function')
    callback = options, options = {};
  else if(typeof options != 'object')
    options = {};

  filepath = options.filePath || gs.internalFilePath;

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
  if(typeof callback == 'function')
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

  function afterStat(err, stats) {
    if(err)
      return gc.errHandler('stats failed ' + filepath, err, ee);
    if(!stats || stats.size == 0)
      return ee.emit('end');
    options.contents ? writeContent(): readFileAndWrite();
  } // << afterStat()

  function writeContent() {
    var buf = typeof options.contents == 'string'
      ? new Buffer(options.contents): options.contents;
    writeBuffer(buf, true);
  } // << writeContent()

  function readFileAndWrite() {

    var stream = (options.stream || fs.createReadStream(filepath, {
      bufferSize: gc.internalChunkSize
    }));

    var careLeak = function() {
      setImmediate(function() {

        stream && stream.removeAllListeners();
        stream = null;

      });
    };

    stream.on('error', function(e) {

      gc.errHandler('write failed ' + filepath, e, ee);
      careLeak();

    }).on('data', function(buf) {

      // breathing for another request
      if((size += buf.length) >= gc.defaultChunkSize)
        stream.pause(), timer.setTimeout(function() {

          size -= gc.defaultChunkSize;
          stream.resume();

        }, parseInt(120 * Math.random()));
      writeBuffer(buf)

    }).on('end', function() {

      writeBuffer(new Buffer(0), true);
      careLeak();

    }).emit('ready');

  } // << readFileAndWrite()

  function writeBuffer(buf, is_last) {
    gc.write(buf, {
      EOF: is_last
    }, function() {
      ee.emit('data', gc.currentChunk.chunkNumber, buf);
      is_last && ee.emit('end');
    });
  } // << writeBuffer

  function last(err) {
    callback(err, gc);
  }

};

/**
 * Save
 */
Commands.prototype.writeFile = function(options, callback) {

  // options and callback are optional.
  var gc = this;

  if(OpenModeIs.write(gc.mode)) {
    util.log('writeFile.', gc.gs.gsid);
    console.log(gc.filename);
  }

  if(typeof options == 'function')
    callback = options, options = {};
  else if(typeof options != 'object')
    options = {};

  /* automatically set in chunkIn()
  if(!options.filePath)
    options.filePath = this.gs.internalFilePath;
  */

  return this.chunkIn(options, callback);
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
Commands.DEFAULT_SAVE_RETRY_MILLISECOND = 800;

/**
 * Write retry interval when writing.
 * 
 * @constant
 */
Commands.DEFAULT_WRITE_RETRY_MILLISECOND = 60;

/**
 * STATS FORCE UPDATE limit.
 * 
 * @constant
 */
Commands.STAT_FORCE_UPDATE_LIMIT_MILLISECOND = 5000;
