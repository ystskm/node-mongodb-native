/**
 * @fileOverview GridFS is a tool for MongoDB to store files
 *               to the database. Because of the
 *               restrictions of the object size the
 *               database can hold, a facility to split a
 *               file into several chunks is needed. The
 *               {@link GridStore} class offers a simplified
 *               api to interact with files while managing
 *               the chunks of split files behind the
 *               scenes. More information about GridFS can
 *               be found <a
 *               href="http://www.mongodb.org/display/DOCS/GridFS">here</a>.
 */
var fs = require('fs'), path = require('path'), util = require('util'), events = require('events');
var Emitter = events.EventEmitter, Stream = require('stream');

// node-mime
var mime = require('mime');

// Set processor, setImmediate if 0.10 otherwise nextTick
var processor = require('../utils').processor();

var REFERENCE_BY_FILENAME = 0, REFERENCE_BY_ID = 1;

// file management events emitted by GridStore.ge
// (not GridStore himself.)
var GsEvent = {
  StatBegin: 'stat_begin',
  StatEnd: 'stat_end',
  WriteWait: 'write_wait_begin',
  WriteUnlock: 'write_unlock',
  StreamPause: 'stream_pause',
  StreamResume: 'stream_resume'
};

// indexes
var Indexes = {
  'fs.files': [
    {
      value: [['_writing', 1], ['filename', 1], ['activation', -1],
        ['expiration', -1]],
      options: {
        unique: true
      }
    }, {
      value: [['filename', 1]]
    }, {
      value: [['_writing', 1], ['_id', 1]]
    }, {
      value: [['uploadDate', -1]]
    }],
  'fs.chunks': [{
    value: [['files_id', 1], ['n', 1]]
  }],
  'fs.conditions': [{
    value: [['_writing', 1]]
  }]
};

/**
 * A class representation of a file stored in GridFS.
 * 
 * <pre type="md">
 * Modes 
 *  - **&quot;r &quot;** - read only. This is the default mode. 
 *  - **&quot;w &quot;** - write in truncate mode. Existing data will be overwriten. 
 *  - **&quot;w+&quot;** - write in edit mode.
 * 
 * Options
 * - **root** {String}, root collection to use.
 *     Defaults to **{GridStore.DEFAULT_ROOT_COLLECTION}**.
 * - **content_type** {String}, mime type of the file.
 *     Defaults to **{GridStore.DEFAULT_CONTENT_TYPE}**.
 * - **chunk_size** {Number}, size for the chunk. 
 *     Defaults to **{Chunk.DEFAULT_CHUNK_SIZE}**. 
 * - **metadata** {Object}, arbitrary data the user wants to store. 
 * - **readPreference** {String}, the prefered read preference 
 *     (ReadPreference.PRIMARY,
 *      ReadPreference.PRIMARY_PREFERRED,
 *      ReadPreference.SECONDARY,
 *      ReadPreference.SECONDARY_PREFERRED,
 *      ReadPreference.NEAREST). 
 * - **w**, {Number/String, &gt; -1 || 'majority' || tag name}, the write concern 
 *     for the operation where &lt; 1 is no acknowlegement of write and 
 *     w &gt;= 1, w = 'majority' or tag acknowledges the write 
 * - **wtimeout**, {Number, 0} set the timeout for waiting for
 *     write concern to finish (combines with w option)
 * - **fsync**, (Boolean, default:false) write waits for fsync
 *     before returning 
 * - **journal**, (Boolean, default:false)
 *     write waits for journal sync before returning
 * </pre>
 * 
 * @class GridStore
 * @param {Db}
 *        db A database instance to interact with.
 * @param {Any}
 *        [id] optional unique id for this file
 * @param {String}
 *        [filename] optional filename for this file, no
 *        unique constrain on the field
 * @param {String}
 *        mode set the mode for this file.
 * @param {Object}
 *        options optional properties to specify.
 * @return {GridStore}
 */
// check index flag
var index_checked = {};

// for connection leak check (CLC) related to : GridstoreCommands
// var opened = 0;
// read-committed enviroment.

var EmitterMap = {};
// EmitterMap[db_name][filename] = handler;

var Queues = {
  forStat: {},
  forWriteWait: {},
  forRead: []
};
// forStat[db_name][filename] = [];

/**
 * @constructor NoLeakEmitter
 */
// leak cared event emitter for one-time use
// TODO require('ystskm/NoleakEmitter')
var NoLeakEmitter = function() {
  var emitter = this;
  Emitter.call(emitter);

  emitter.on('error', careLeak).on('end', careLeak);
  function careLeak() {
    processor(function() {

      emitter && emitter.removeAllListeners();
      emitter = null;

    });
  }
};
util.inherits(NoLeakEmitter, Emitter);

// for debug
var gsid = 0, memory = null;

/**
 * @require GridStoreCommands
 */
var GridStoreCommands = require('./gridstore_command');

/**
 * @exports GridStore
 */
exports.GridStore = GridStore;

/**
 * @constructor GridStore
 */
function GridStore(db, fileTarget, mode, options) {

  var gs = this;
  gs.db = db;

  // Call stream constructor
  Stream.call(gs);

  gs.gsid = gsid++ & 0xffffff;
  gs.q = Queues;

  // for debug
  //console.log('[new GridStore] ' + this.gsid);

  // set grid referencetype
  if(fileTarget instanceof db.bson_serializer.ObjectID) {

    gs.referenceBy = REFERENCE_BY_ID;
    gs.filesId = fileTarget, gs.filename = null;

  } else {

    gs.referenceBy = REFERENCE_BY_FILENAME;
    gs.filesId = null, gs.filename = fileTarget;

  }

  gs.max_memory = options.max_memory_use_for_readstream
    || GridStore.DEFAULT_MAX_MEMORY_USE_FOR_READSTREAM;
  if(memory == null)
    memory = gs.max_memory;

  // Set up the rest
  gs.mode = mode || 'r';
  gs.options = options || {};

  gs.closedb = db.serverConfig.isConnected() ? false
    : gs.options.closedb || false;

  gs.root = gs.options.root || GridStore.DEFAULT_ROOT_COLLECTION;
  // "fs."

  gs.safe = gs.options.safe == null ? gs: gs.options.safe;
  // effects to mode "r" only ?

  // TODO now clear option is not affected?
  if(gs.options.clear)
    gs.safe = false;

  gs.contentType = gs.options.contentType || mime.lookup(gs.filename)
    || GridStore.DEFAULT_CONTENT_TYPE;

  gs.defaultChunkSize = (gs.options.chunkSize || GridStore.DEFAULT_CHUNK_SIZE)
    - GridStore.MAX_CHUNK_HEADER_SIZE;

  // saveName > virtualPath > actualPath for save "filename"
  if(gs.options.saveName)
    gs.filename = gs.options.saveName;

  // filePath > actualPath > filename for read "internalFilePath"
  if(gs.options.filePath)
    gs.internalFilePath = gs.options.filePath;

  if(gs.options.virtualPath) {
    gs.internalVirtualPath = path.resolve(gs.options.virtualPath, fileTarget);

    if(gs.filename == fileTarget && !gs.options.saveName)
      gs.filename = gs.internalVirtualPath;
  }

  if(gs.options.actualPath) {
    gs.internalFilePath = path.resolve(gs.options.actualPath, fileTarget);

    if(gs.filename == fileTarget && !gs.options.saveName)
      gs.filename = gs.internalFilePath;
  }

  if(!gs.internalFilePath)
    gs.internalFilePath = gs.filename;

  // extension status
  gs.metadata = gs.options.metadata;

  var dbnm = db.databaseName, flnm = gs.filename;
  gs.ge = getListeningEmitter(dbnm, flnm);

  if(!gs.safe && 'r' == gs.mode)
    db.slaveOk = false;

  // for DEBUG >>
  //  console.log('FilePath:', this.internalFilePath);
  //  console.log('VirtualPath:', this.internalVirtualPath);
  //  console.log('SaveName:', this.filename);
  // <<

  /**
   * The md5 checksum for this file.
   * 
   * @name md5
   * @lends GridStore
   * @field
   */

  // TODO check mean and modify.
  // !! should care memory leak !!
  gs.__defineGetter__("md5", function() {
    return gs.internalMd5;
  });

  gs.__defineSetter__("md5", function(value) {
  });

};

function getListeningEmitter(dbnm, flnm) {

  // checking event map and queue for each file
  if(EmitterMap[dbnm] == null)
    EmitterMap[dbnm] = {};
  ['forStat', 'forWriteWait'].forEach(function(key) {
    if(Queues[key][dbnm] == null)
      Queues[key][dbnm] = {};
  });

  // returns emitter if already exist
  if(EmitterMap[dbnm][flnm])
    return EmitterMap[dbnm][flnm];

  var emitter = new Emitter();

  //assign event
  [{
    qname: 'forStat',
    se_name: GsEvent.StatBegin,
    ee_name: GsEvent.StatEnd
  }, {
    qname: 'forWriteWait',
    se_name: GsEvent.WriteWait,
    ee_name: GsEvent.WriteUnlock
  }].forEach(function(info) {

    //stats queue
    var qname = info.qname;
    Queues[qname][dbnm][flnm] = [];

    //when stats start, queue and if only me, kick function
    emitter.on(info.se_name, function(fn) {
      var q = Queues[qname][dbnm][flnm];

      q.push(fn);
      //__consoleQueue('BEG', qname);

      var len = q.length;
      // if(/file-io\/index.js/.test(flnm))
      // if(len > 1)
      // util.log('III[' + info.qname + '] l(' + len + ') f(' + flnm + ')');

      // if only me, execute
      if(len == 1)
        process.nextTick(q[0]);
    });

    //when statsend, next stats update function call
    emitter.on(info.ee_name, function() {
      var q = Queues[qname][dbnm][flnm];

      q.shift();
      //__consoleQueue('END', qname);

      var len = q.length;
      // if(/file-io\/index.js/.test(flnm))
      // if(len > 1)
      // util.log('OOO[' + info.qname + '] l(' + q.length + ') f(' + flnm + ')');

      // if more exist, execute
      if(len)
        process.nextTick(q[0]);
    });

  });
  // assign new emitter
  return EmitterMap[dbnm][flnm] = emitter;

}
/**
 * Code for the streaming capabilities of the gridstore
 * object Most code from Aaron heckmanns project
 * https://github.com/aheckmann/gridfs-stream Modified to
 * work on the gridstore object itself
 * 
 * @ignore
 */
if(typeof Stream == 'function') {
  GridStore.prototype = {
    __proto__: Stream.prototype
  }
}

GridStore.prototype.useMemory = function(v, fn) {
  // TODO
};

GridStore.prototype.releaseMemory = function() {
  // TODO
};

/**
 * Opens the file from the database and initialize this
 * object. Also creates a new one if file does not exist.
 * 
 * @param {Function}
 *        callback this will be called after executing this
 *        method. The first parameter will contain an
 *        **{Error}** object and the second parameter will
 *        be null if an error occured. Otherwise, the first
 *        parameter will be null and the second will contain
 *        the reference to this object.
 * @return {null}
 * @api public
 */
GridStore.prototype.open = function(callback) {
  var gs = this, db = gs.db;

  if(gs.mode != "w" && gs.mode != "w+" && gs.mode != "r") {
    callback(new Error("Illegal mode " + gs.mode), null);
    return;
  }

  db.open(function(err) {

    // for connection leak check (CLC) >>
    /*
    if(!GridStore.Connections)
      GridStore.Connections = {};
    if(!self._openNum) {
      self._openNum = ++opened;
      GridStore.Connections[self._openNum] = true;
      console.log('gridstore:' + self._openNum + ' opened. (' + Object.keys(GridStore.Connections) + ' left.)');
    } else {
      console.log('gridstore:' + 'complex open' + self._openNum + '(' + Object.keys(GridStore.Connections) + ' left.)');
    }
    */
    // << CLC
    // on error
    if(err)
      return callback(err, null);

    //endOfOpen();
    indexCheck(db, endOfOpen);

  });

  function endOfOpen() {
    new GridStoreCommands(gs).open(function(err) {
      if(err)
        return gs.error(err, callback);

      // "this" is GriStoreCommands instance
      callback(null, this);
    });
  }

};

function indexCheck(db, callback) {

  var dbnm = db.databaseName;

  // if check is already done, let's go next.
  if(index_checked[dbnm] === true)
    return callback();

  // if in checking progress, push to the waiting queue
  if(Array.isArray(index_checked[dbnm]))
    return index_checked[dbnm].push(callback);

  index_checked[dbnm] = [];

  // CAN'T USE "NoLeakEmitter" ,for assume that multiple "end" event.
  var progress_ns = {}, progress_ee = new Emitter();
  progress_ee.on('end', function(ns) {
    util.log('[gridstore] Indexes check end.(ns: ' + ns + ')');

    if(--progress_ns[ns] == 0)
      delete progress_ns[ns];

    if(Object.keys(progress_ns).length == 0) {

      progress_ee.removeAllListeners();
      progress_ee = null, callback();

      index_checked[dbnm].forEach(function(waitingCallback) {
        waitingCallback();
      });

      index_checked[dbnm] = true;
      util.log('[gridstore] Indexes checking completed .(db: ' + dbnm + ')');

    }

  });

  for( var ns in Indexes)
    (function(ns, indexes) {

      if(!progress_ns[ns])
        progress_ns[ns] = 0;

      progress_ns[ns]++, checkIndexes(ns, indexes);

    })(ns, Indexes[ns]);

  function checkIndexes(ns, indexes) {

    var col = null;
    getCurrent();

    function getCurrent() {
      db.createCollection(ns, function(err, _col) {
        col = _col, findSysIndexCol();
      });
    }

    function findSysIndexCol() {

      db.createCollection('system.indexes', function(err, sys_col) {
        sys_col.find({
          ns: col.db.databaseName + '.' + ns
        }, checkDiff);
      });

    }

    function checkDiff(err, cur) {

      var dropKeyPatterns = [];

      cur.each(function(err, doc) {

        if(!doc)
          return execDrop();

        var keys = Object.keys(doc.key), delete_index = true;
        if(keys.length == 1 && doc.key._id)
          return;

        for( var i = 0; i < indexes.length; i++) {

          if(!indexes[i].ready)
            indexes[i].ready = {};

          if(indexes[i].ready[dbnm])
            continue;

          var sets = indexes[i].value, opts = indexes[i].options;

          if(keys.length != sets.length)
            continue;

          var found = true;

          // keys check
          for( var odr = 0; odr < keys.length; odr++)
            if(!sets[odr] || sets[odr][0] != keys[odr]
              || sets[odr][1] != doc.key[keys[odr]])
              found = false;

          // options check
          ['sparse', 'unique', 'dropDups', 'background'].forEach(function(k) {

            if((!opts || !opts[k]) && doc[k])
              return found = false;

            if(opts && opts[k] && !doc[k])
              return found = false;

          });

          if(!found)
            continue;

          delete_index = false, indexes[i].ready[dbnm] = true;

        }

        if(delete_index)
          dropKeyPatterns.push(doc.key);

      });

      function execDrop() {

        var ope_cnt = dropKeyPatterns.length;

        if(ope_cnt == 0)
          execEnsure();

        dropKeyPatterns.forEach(function(patt) {
          clear(patt, function() {

            if(--ope_cnt == 0)
              execEnsure();

          });
        });

      }

      function execEnsure() {

        var ope_cnt = 0;

        indexes.forEach(function(index) {
          if(index.ready && index.ready[dbnm])
            return delete index.ready[dbnm];

          ope_cnt++;
        });

        if(ope_cnt == 0)
          return (dropKeyPatterns.length == 0 ? endOfNs: re)();

        indexes.forEach(function(index) {
          ensure(index, function() {

            if(--ope_cnt == 0)
              re();

          });
        });

      }

    }

    function clear(patt, callback) {
      util.log('[gridstore] dropIndex:' + JSON.stringify(patt) + ', from: '
        + ns);

      col.dropIndex(patt, callback);
    }

    function ensure(index, callback) {
      util.log('[gridstore] ensureIndex:' + JSON.stringify(index.value)
        + ', to: ' + ns);

      col.ensureIndex(index.value, index.options, callback);
    }

    function re() {
      util.log('[gridstore] execute reIndex now, to ' + ns);
      db.executeDbCommand({

        reIndex: ns

      }, endOfNs);
    }

    function endOfNs() {
      progress_ee.emit('end', ns);
    }

  }

}

// close method
GridStore.prototype.close = function(afterClose) {
  var gs = this, db = gs.db;

  if(gs.closedb == true && db.serverConfig.isConnected())
    return db.close(endOfClose);
  endOfClose();

  function endOfClose(err) {

    // callback at first
    afterClose(err);

    // leak care logic
    processor(function() {

      // release stream handlers
      gs.removeAllListeners();

      // release objects and internal recursive reference
      ['db', 'q', 'ge', 'options', 'md5'].forEach(function(k) {
        delete gs[k];
      });

      // release local
      gs = db = null;

    });

  }
};

// namespace getter for CLC
/*
GridStore.prototype.namespace = function(){
  return GridStore;
}
*/

// events 
GridStore.prototype.statBegin = function(fn) {
  var ee = this.ge;
  ee.emit.call(ee, GsEvent.StatBegin, fn);
};

GridStore.prototype.statEnd = function() {
  var ee = this.ge;
  ee.emit.call(ee, GsEvent.StatEnd);
};

GridStore.prototype.waitForWrite = function(fn) {
  var ee = this.ge;
  ee.emit.call(ee, GsEvent.WriteWait, fn);
};

GridStore.prototype.informWriteUnlock = function() {
  var ee = this.ge;
  ee.emit.call(ee, GsEvent.WriteUnlock);
};

GridStore.prototype.error = function(occuredError, afterClose) {
  var gs = this;

  // close connection if connected
  gs.close(function(err) {

    // output close error to console
    if(err)
      console.error(err);

    if(typeof afterClose == 'function')
      return afterClose(occuredError);

    throw occuredError;

  });
};

/**
 * Saves this file to the database. This will overwrite the
 * old entry if it already exists. This will work properly
 * only if mode was initialized to "w" or "w+".
 */
GridStore.prototype.writeFile = function(filepath, options, callback) {
  var gs = this;

  // all arguments are optional.
  // filepath can be string. If options.contents is found, replace to that.
  var gc = null;

  // initialize arguments
  if(typeof options == 'function') {

    callback = options;

    if(typeof filepath == 'object')
      options = filepath, filepath = null;
    else
      options = {};

  } else if(typeof filepath == 'function') {

    callback = filepath, options = {}, filepath = null;

  }

  if(filepath === null)
    filepath = gs.internalFilePath;

  if(options.clear == true && gs.safe == true)
    return last(new Error('You should open file with "' + 'safe = false'
      + '" when use clear option'));

  if(!options.filePath)
    options.filePath = filepath;

  gs.open(opencb);

  function opencb(err, commands) {
    if(err)
      return last(err);

    (gc = commands).writeFile(options, close);
  }

  function close(err) {
    if(err)
      return last(err);

    // gc.close() always call gs.close()
    gc.close(last);
  }

  function last(err) {
    callback(err, null);
  }

};

/**
 * The default chunk size
 * 
 * @constant
 */
// NOTICE: max-chunk-size of mongodb v2.2.3, v2.4.3 is 4MB but 8MB of v1.x
// "1024" means header space for a chunk document
GridStore.DEFAULT_CHUNK_SIZE = 1024 * 1024 * 4;
GridStore.MAX_CHUNK_HEADER_SIZE = 1024;
//for test
//GridStore.DEFAULT_CHUNK_SIZE = 256;
/**
 * The default max memory use for stream write
 * 
 * @constant
 */
GridStore.DEFAULT_MAX_MEMORY_USE_FOR_READSTREAM = 1024 * 1024 * 512;

/**
 * The collection to be used for holding the files and
 * chunks collection.
 * 
 * @constant
 */
GridStore.DEFAULT_ROOT_COLLECTION = 'fs';

/**
 * Default file mime type
 * 
 * @constant
 */
GridStore.DEFAULT_CONTENT_TYPE = 'application/octet-stream';

/**
 * Seek mode where the given length is absolute.
 * 
 * @constant
 */
GridStore.IO_SEEK_SET = 0;

/**
 * Seek mode where the given length is an offset to the
 * current read/write head.
 * 
 * @constant
 */
GridStore.IO_SEEK_CUR = 1;

/**
 * Seek mode where the given length is an offset to the end
 * of the file.
 * 
 * @constant
 */
GridStore.IO_SEEK_END = 2;

/**
 * directly callable functions
 */
GridStore.unlink = function(db, fp, callback) {

  new GridStore(db, fp, 'w').open(opencb);

  function opencb(err, gc) {
    if(err)
      return closed(err);

    gc.deleteFile(delcb);
  }

  function delcb(err, gc) {
    gc.close(function(err2) {

      closed(err || err2);

    });
  }

  function closed(err) {
    callback(err);
  }

};

GridStore.exist = function(db, name, callback) {

  var status = null;
  new GridStore(db, name, 'r').open(opencb);

  function opencb(err, gc) {
    if(err)
      return closed(err);

    gc.stat(statcb);
  }

  function statcb(err, d, gc) {
    status = d, gc.close(function(err2) {

      closed(err || err2);

    });
  }

  function closed(err) {
    var exist = true;

    if(err != null || status == null || status._writing != 0)
      exist = false;

    callback(err, exist);
  }

};

GridStore.ls = function(db, dirpath, options, callback) {

  if(typeof options == 'function')
    callback = options, options = {};

  if(options == null)
    options = {};

  if(options.skip == null)
    options.skip = 0; // [hack] not to treat as fields in mongodb-native.

  // "optons.depth" is set
  // it's the sign of directory-depth specify search
  var ext_search_mode = false;
  var depth = options.depth;
  if(depth != null)
    ext_search_mode = true;

  // directory array
  var rega = dirpath.split('/').map(function(n) {
    return n.indexOf('*') == -1 ? n: n.replace(/\*/g, '[^\\/]*');
  });

  // '/' is set for dirpath care
  var regl = (rega.slice(-1).toString()).trim();
  regl.length === 0 && rega.pop();

  // filename option fix
  var filename = options.filename;
  if(filename) {
    filename = String(filename).replace(/([^\\])\./g, '$1\\.');
    filename = filename.replace(/\*/g, '[^\\/]*');
  }

  var emitter = new NoLeakEmitter();
  if(typeof callback == 'function') {
    var ret = [];
    emitter.on('error', callback).on('data', function(data) {
      ret.push(data);
    }).on('end', function() {
      callback(null, ret);
    });
  }

  var regs = '^' + rega.join('\\/');

  if(ext_search_mode) {

    // change "depth" condition to Array(2)
    depth = String(depth).split(',').map(function(d) {
      // MongoDB Index Key Limit requires LESS THAN 1024 BYTES
      // so, the longest directory depth < 512 (e.g. "gridfs/a/b/...")
      d = parseInt(d);
      return isNaN(d) ? '*': Math.min(d, 512) || 0;
    }).slice(0, 2).sort(function(a, b) {
      return a < b ? -1: 1;
    });
    if(depth.length == 1)
      depth.push('*');

    // change directory regexp for each "depth" condition case
    if('*' == depth[0])
      regs += '(\\/[^\\/]+)*';
    else if('*' == depth[1])
      regs += depth[0] ? '(\\/[^\\/]+){,' + depth[0] + '}': '';
    else if(depth[0] === depth[1])
      regs += depth[0] ? '(\\/[^\\/]+){' + depth[0] + '}': '';
    else
      regs += '(\\/[^\\/]+){' + depth[0] + ',' + depth[1] + '}';
    // else depth <= 0, no-regs change.
  }

  // set file regular expression
  regs += '\\/' + (filename || '[^\\/]+') + '$';

  var selector = {
    /**
     * @see gridstore_commands _writing 0: FILE_IS.STABLE 1:
     *      FILE_IS.BEFORE_STABLE FILE_IS_BEFORE_DELETE:3
     */
    _writing: {
      $in: [0, 1]
    },

    filename: new RegExp(regs),
  };

  var root = options['root'] || GridStore.DEFAULT_ROOT_COLLECTION;
  var client = null, store = {};

  // slow
  /*
  setTimeout(function() {

    db.open(opencb);

  }, 0);
  */
  process.nextTick(function() {
    db.open(opencb);
  });

  return emitter;

  function opencb(err, cl) {
    client = cl;
    db.collection(root + '.files', collcb);
  }

  function collcb(err, col) {
    col.find(selector, options, function(err, cur) {
      cur.each(findcb);
    });
  }

  function findcb(err, doc) {

    if(err)
      return last(err);

    if(!doc)
      return emit();//End of cursor

    if(store[doc.filename] === false)
      return;

    if(doc._writing == 0)
      if(!store[doc.filename])
        return store[doc.filename] = doc;

    if(doc._writing == 1)
      return store[doc.filename] = doc;

    store[doc.filename] = false;

  }

  function emit() {
    for( var name in store)
      store[name] && emitter.emit('data', store[name]);
    last();
  }

  function last(err) {
    // soft close
    client.close(function() {
      err ? emitter.emit('error', err): emitter.emit('end');
    });

  }

};

/**
 * @ignore
 * @api private A function for debugging waiting queues.
 */
function __consoleQueue(type, qname) {
  var cnt = 0;

  console.log('[' + type + '.' + qname + ']>>>> ====================== >>>>');
  ['forStat', 'forWriteWait'].forEach(function(key) {
    var qk = Queues && (Queues[key] || {});

    var qdb = null, qfl = null;
    for( var db in qk) {
      qdb = qk[db] || {};
      for( var fl in qdb) {
        qfl = qdb[fl] || [], num = qfl.length, cnt += num;
        console.log(key + '=' + num + ': ' + db + '.' + fl)
      }
    }

  });
  console.log(cnt + ' <<<< ====================== <<<<')
}
