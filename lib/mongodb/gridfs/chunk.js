var Binary = require('bson').Binary,
  ObjectID = require('bson').ObjectID;

/**
 * Class for representing a single chunk in GridFS.
 *
 * @class
 *
 * @param file {GridStore} The {@link GridStore} object holding this chunk.
 * @param mongoObject {object} The mongo object representation of this chunk.
 *
 * @throws Error when the type of data field for {@link mongoObject} is not
 *     supported. Currently supported types for data field are instances of
 *     {@link String}, {@link Array}, {@link Binary} and {@link Binary}
 *     from the bson module
 *
 * @see Chunk#buildMongoObject
 */
var Chunk = exports.Chunk = function(gridcommands, chunkParam) {
  if(!(this instanceof Chunk)) return new Chunk(gridcommands, chunkParam);

  this.gc = gridcommands;

  var serializer = this.gc.db.bson_serializer;
  this.mongoObj = chunkParam || {};
  this.objectId = this.mongoObj._id || new serializer.ObjectID();
  this.chunkNumber = this.mongoObj.n || 0;
  this.chunkSize = this.mongoObj.chunkSize || this.gc.internalChunkSize;

  if(this.mongoObj.data == null)
    this.binary = new serializer.Binary();
  else if(this.mongoObj.data.constructor == String) {
    var buffer = new Buffer(this.mongoObj.data.length);
    buffer.write(this.mongoObj.data, 'binary', 0);
    this.binary = new serializer.Binary(buffer);
  } else if(this.mongoObj.data.constructor == Array) {
    var buffer = new Buffer(this.mongoObj.data.length);
    buffer.write(this.mongoObj.data.join(''), 'binary', 0);
    this.binary = new serializer.Binary(buffer);
  } else if(this.mongoObj.data instanceof serializer.Binary
    || Object.prototype.toString.call(this.mongoObj.data) == "[object Binary]")
    this.binary = this.mongoObj.data;
  else if(this.mongoObj.data instanceof Buffer)
    this.binary = new serializer.Binary(this.mongoObj.data);
  else {
    throw Error("Illegal chunk format");
  }
  // Update position
  this.internalPosition = this.length() || 0;
};

/**
 * Writes a data to this object and advance the read/write head.
 *
 * @param data {string} the data to write 
 * @param callback {function(*, GridStore)} This will be called after executing
 *     this method. The first parameter will contain null and the second one
 *     will contain a reference to this object.
 */
Chunk.prototype.write = function(data, callback) {
  var self = this;
  this.binary.write(data, this.internalPosition);
  this.internalPosition = this.length();
  process.nextTick(last);
  function last() {
    callback(null, self);
  }
};

/**
 * Reads data and advances the read/write head.
 *
 * @param length {number} The length of data to read.
 *
 * @return {string} The data read if the given length will not exceed the end of
 *     the chunk. Returns an empty String otherwise.
 */
Chunk.prototype.read = function(length, strict) {
  // Default to full read if no index defined
  length = length == null || length == 0 ? this.length(): length;
  strict = strict == null ? false: true;
  if(this.length() - this.internalPosition + 1 >= length) {
    var data = this.binary.read(this.internalPosition, length);
    this.internalPosition = this.internalPosition + length;
    return data;
  } else if(!strict && this.length() - this.internalPosition + 1 >= 0) {
    var data = this.binary.read(this.internalPosition, this.length()
      - this.internalPosition + 1);
    this.internalPosition = this.length() - 1;
    return data;
  } else {
    return '';
  }
};

Chunk.prototype.readSlice = function(length) {
  if((this.length() - this.internalPosition) >= length) {
    var data = null;
    if(this.data.buffer != null) { //Pure BSON
      data = this.data.buffer.slice(this.internalPosition,
        this.internalPosition + length);
    } else { //Native BSON
      data = new Buffer(length);
      length = this.data.readInto(data, this.internalPosition);
    }
    this.internalPosition = this.internalPosition + length;
    return data;
  } else {
    return null;
  }
};

/**
 * Checks if the read/write head is at the end.
 *
 * @return {boolean} Whether the read/write head has reached the end of this
 *     chunk.
 */
Chunk.prototype.eof = function() {
  return this.internalPosition == this.length() ? true : false;
};

/**
 * Reads one character from the data of this chunk and advances the read/write
 * head.
 *
 * @return {string} a single character data read if the the read/write head is
 *     not at the end of the chunk. Returns an empty String otherwise.
 */
Chunk.prototype.getc = function() {
  return this.read(1);
};

/**
 * Clears the contents of the data in this chunk and resets the read/write head
 * to the initial position.
 */
Chunk.prototype.rewind = function() {
  this.internalPosition = 0;
  this.binary = new this.gc.db.bson_serializer.Binary();
};

/**
 * Saves this chunk to the database. Also overwrites existing entries having the
 * same id as this chunk.
 *
 * @param callback {function(*, GridStore)} This will be called after executing
 *     this method. The first parameter will contain null and the second one
 *     will contain a reference to this object.
 */
Chunk.prototype.save = function(callback) {

  var self = this, collection = null;
  this.gc.chunksCollection(function(err, col) {
    // remove same position chunk
    collection = col, collection.remove({
      'files_id': self.gc.filesId,
      'n': self.position(),
      safe: true
    }, rmcb);
  });

  // rebuildMongoObject
  function rmcb(err, result) {
    self.buildMongoObject(mocb);
  }

  // insert chunk
  function mocb(mongoObject) {
    collection.save(mongoObject, {
      safe: true
    }, last);
  }

  // callback
  function last(err, collection) {
    callback(err, self);
  };

};

/**
 * Creates a mongoDB object representation of this chunk.
 *
 * @param callback {function(Object)} This will be called after executing this 
 *     method. The object will be passed to the first parameter and will have
 *     the structure:
 *        
 *        <pre><code>
 *        {
 *          '_id' : , // {number} id for this chunk
 *          'files_id' : , // {number} foreign key to the file collection
 *          'n' : , // {number} chunk number
 *          'data' : , // {bson#Binary} the chunk data itself
 *        }
 *        </code></pre>
 *
 * @see <a href="http://www.mongodb.org/display/DOCS/GridFS+Specification#GridFSSpecification-{{chunks}}">MongoDB GridFS Chunk Object Structure</a>
 */
Chunk.prototype.buildMongoObject = function(callback) {
  var mongoObject = {
    '_id': this.objectId,
    'files_id': this.gc.filesId,
    'n': this.chunkNumber,
    'data': this.binary
  };
  // asynchronize'd
  return typeof callback == 'function' ? process.nextTick(last): mongoObject;
  function last() {
    callback(mongoObject);
  }
};

/**
 * @return {number} the length of the data
 */
Chunk.prototype.length = function() {
  return this.binary.length();
};

/**
 * @return {number} the position of the Chunk
 */
Chunk.prototype.position = function() {
  return this.chunkNumber;
}
/**
 * The position of the read/write head
 * @name position
 * @lends Chunk#
 * @field
 */
/*
Object.defineProperty(Chunk.prototype, "position", { enumerable: true
  , get: function () {
      return this.internalPosition;
    }
  , set: function(value) {
      this.internalPosition = value;
    }
});*/

/**
 * The default chunk size
 * @constant
 */
Chunk.DEFAULT_CHUNK_SIZE = 1024 * 256;
