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

// noleak-emitter
var NoleakEmitter = require('noleak-emitter');

// Set processor, setImmediate if 0.10 otherwise nextTick
var processor = require('../utils').processor();
var REFERENCE_BY_FILENAME = 0, REFERENCE_BY_ID = 1;

// file management events emitted by GridStore.ge
// (not GridStore himself.)

// !! PERFECTLY NON_BLOCKING IO ~ 2015/03/23 !!
var GsEvent = {
  StreamPause: 'stream_pause',
  StreamResume: 'stream_resume'
};

// indexes
var Indexes = {

  // make file list
  'fs.files': [
    {
      value: [['filename', 1], ['activation', -1], ['expiration', -1],
        ['_no', 1]],
      options: {
        unique: true
      }
    }, {
      value: [['_writing', 1], ['filename', 1], ['_no', -1]]
    }, {
      value: [['uploadDate', -1]]
    }],

  // make file content chunks
  'fs.chunks': [{
    value: [['files_id', 1], ['n', 1]]
  }],

  // make file seq manager
  // _id: gridfs position, _no: max sequential number for the file
  'fs.conditions': []

};

/**
 * A class representation of a file stored in GridFS.
 * 
 * Modes - **"r"** - read only. This is the default mode. -
 * **"w"** - write in truncate mode. Existing data will be
 * overwriten. - **w+"** - write in edit mode.
 * 
 * Options - **root** {String}, root collection to use.
 * Defaults to **{GridStore.DEFAULT_ROOT_COLLECTION}**. -
 * **content_type** {String}, mime type of the file.
 * Defaults to **{GridStore.DEFAULT_CONTENT_TYPE}**. -
 * **chunk_size** {Number}, size for the chunk. Defaults to
 * **{Chunk.DEFAULT_CHUNK_SIZE}**. - **metadata** {Object},
 * arbitrary data the user wants to store. -
 * **readPreference** {String}, the prefered read preference
 * (ReadPreference.PRIMARY,
 * ReadPreference.PRIMARY_PREFERRED,
 * ReadPreference.SECONDARY,
 * ReadPreference.SECONDARY_PREFERRED,
 * ReadPreference.NEAREST). - **w**, {Number/String, > -1 ||
 * 'majority' || tag name} the write concern for the
 * operation where < 1 is no acknowlegement of write and w >=
 * 1, w = 'majority' or tag acknowledges the write -
 * **wtimeout**, {Number, 0} set the timeout for waiting for
 * write concern to finish (combines with w option) -
 * **fsync**, (Boolean, default:false) write waits for fsync
 * before returning - **journal**, (Boolean, default:false)
 * write waits for journal sync before returning
 * 
 * @class Represents the GridStore.
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

  // for traceability
  gs.gsid = gsid++ & 0xffffff;

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

  // overwrite concern for a file.
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
  gs.md5 = gs.internalMd5;
  //  gs.__defineGetter__("md5", function() {
  //    return gs.internalMd5;
  //  });
  //
  //  gs.__defineSetter__("md5", function(value) {
  //  });

};

/**
 * Code for the streaming capabilities of the gridstore
 * object Most code from Aaron heckmanns project
 * https://github.com/aheckmann/gridfs-stream Modified to
 * work on the gridstore object itself
 * 
 * @ignore
 */
util.inherits(GridStore, Stream);

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

  db.open(function(er) {

    // on error
    if(er)
      return callback(er, null);

    //endOfOpen();
    indexCheck(db, endOfOpen);

  });

  function endOfOpen() {
    new GridStoreCommands(gs).open(function(er, gc) {

      if(er)
        return gs.error(er, callback);

      // "gc" is GriStoreCommands instance
      callback(null, gc);

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

  // CAN'T USE "NoleakEmitter" ,for assume that multiple "end" event.
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

      // TODO fix "Cannot read property 'db' of null"
      db.createCollection('system.indexes', function(err, sys_col) {
        sys_col.find({
          ns: col.db.databaseName + '.' + ns
        }, checkDiff);
      });

    }

    function checkDiff(err, cur) {

      var dropKeyPatterns = [];

      cur.each(function(err, doc) {

        if(!doc) {
          cur.rewind(), cur = null;
          return execDrop();
        }

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
      ['db', 'options', 'md5'].forEach(function(k) {
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

  function opencb(er) {
    if(er)
      return last(er);
    (gc = this).writeFile(options, close);
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

  function opencb(er, gc) {
    if(er)
      return callback(er);
    gc.deleteFile(delcb);
  }

  function delcb(er, gc) {
    gc.close(function(er2) {

      callback(er || er2);

    });
  }

};

/**
 * exist functions
 */
GridStore.exist = function(db, name, callback) {

  var status = null;
  new GridStore(db, name, 'r').open(opencb);

  function opencb(er, gc) {
    if(er)
      return closed(er);
    gc.stat(statcb);
  }

  function statcb(er, d, gc) {
    status = d, gc.close(function(er2) {
      closed(er || er2);
    });
  }

  function closed(er) {
    var exist = !(er != null || status == null || status._writing != 0)
    callback(er, exist);
  }

};

/**
 * mv functions
 */
GridStore.mv = function(db, source, target, callback) {

  if(typeof source != 'string')
    return callback(new Error('Source must be a string.'))
  if(typeof target != 'string')
    return callback(new Error('Target must be a string.'))

  var root = options['root'] || GridStore.DEFAULT_ROOT_COLLECTION;
  var client = null;

  process.nextTick(checkSource);
  return emitter;

  function checkSource() {
    GridStore.exist(db, source, function(err, exist) {

      if(err)
        return last(err);
      if(!exist)
        return last(new Error('Source file is not exist.'));

      checkTarget();
    });
  }

  function checkTarget() {
    GridStore.exist(db, target, function(err, exist) {

      if(err)
        return last(err);
      if(exist)
        return last(new Error('Target file is already exist.'));

      mv();
    });
  }

  function mv() {
    db.open(function(err, cl) {

      if(err)
        return last(err);
      client = cl;

      db.collection(root + '.files', function(err, col) {
        if(err)
          return last(err);
        update();
      });

    });
  }

  function update() {
    col.update({
      filename: source
    }, {
      $set: {
        filename: target
      }
    }, updateCallback);
  }

  function updateCallback(err, cnt) {

    if(err)
      return last(err);
    if(cnt !== 1)
      return last(new Error('mv command failed.(' + cnt + ')'))

    emitter.emit('data', cnt);
    last();

  }

  function last(err) {

    // soft close
    client ? client.close(emit): setImmediate(emit);

    // emit via emitter
    function emit() {
      err ? emitter.emit('error', err): emitter.emit('end');
    }

  }

};

/**
 * ls functions
 */
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

  var emitter = new NoleakEmitter();
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
      regs += depth[0] ? '(\\/[^\\/]+){0,' + depth[0] + '}': '';
    else if(depth[0] === depth[1])
      regs += depth[0] ? '(\\/[^\\/]+){' + depth[0] + '}': '';
    else
      regs += '(\\/[^\\/]+){' + depth[0] + ',' + depth[1] + '}';
    // else depth <= 0, no-regs change.
  }

  // set file regular expression
  regs += '\\/' + (filename || '[^\\/]+') + '$';

  /**
   * @see gridstore_commands _writing 0: FILE_IS.STABLE
   */
  var selector = {
    _writing: 0,
    filename: new RegExp(regs)
  };

  var root = options['root'] || GridStore.DEFAULT_ROOT_COLLECTION;
  options.sort = options.sort || {};

  options.sort['filename'] = options.sort['filename'] || 1;
  options.sort['_no'] = -1;

  var client = null, store = {};
  process.nextTick(function() {
    db.open(opencb);
  });

  return emitter;

  function opencb(err, cl) {
    client = cl;
    db.collection(root + '.files', collcb);
  }

  function collcb(err, col) {
    col.find(selector, options, findcb);
  }

  function findcb(err, cur) {

    if(err) {
      cur.rewind(), cur = null;
      return last(err);
    }

    cur.each(function(err, doc) {

      if(err) {
        cur.rewind(), cur = null;
        return last(err);
      }

      if(!doc) {
        cur.rewind(), cur = null;
        return emit();//End of cursor
      }

      if(store[doc.filename])
        return;

      if(!store[doc.filename])
        return store[doc.filename] = doc;

    });
  }

  function emit() {
    for( var fp in store)
      store[fp] && emitter.emit('data', store[fp]);
    last();
  }

  function last(er) {

    // soft close
    client ? client.close(emit): setImmediate(emit);

    // emit via emitter
    function emit() {
      er ? emitter.emit('error', er): emitter.emit('end');
    }

  }

};
