/** An OffsetCache keeps track of how many elements in an HDT file have been removed up to a certain offset **/
var N3 = require('n3');

function OffsetCache(options) {
  options = options || {};
  this._queryLimit = options.query_limit || 150; // Default maximum number of queries to keep
  this._offsetLimit = options.offset_limit || 500; // Default maximum number of offsets per query
  this.flushCache();
}

OffsetCache.prototype.flushCache = function() {
  this._queryCache = {};
  this._qrList = [];
  this._queryCount = 0;
};

// Returns a random number between min (inclusive) and max (exclusive)
OffsetCache.prototype._getRandomInt = function(min,max) {
  return Math.floor(Math.random() * (max - min)) + min;
};

// Removes one query at random from the cache, to keep memory usage low
OffsetCache.prototype._cleanupQuery = function() {
  if(this._qrList.length < this._queryLimit) return;
  var qrIdx = this._getRandomInt(0,this._qrList.length),
      qrId = this._qrList[qrIdx];
  this._queryCache[qrId] = undefined;
  this._qrList.splice(qrIdx,1);
  this._queryCount -= 1;
};

// Removes one offset at random from each query, to keep memory usage low
OffsetCache.prototype._cleanupOffset = function(id) {
  var len = this._queryCache[id].virtual.length;
  if(len < this._offsetLimit) return;

  var target = this._getRandomInt(0,len);
  this._queryCache[id].virtual.splice(target,1);
  this._queryCache[id].real.splice(target,1);
};

OffsetCache.prototype._initQuery = function(query) {
  this._cleanupQuery();
  var id = this.getId(query);
  this._queryCache[id] = {};
  this._queryCache[id].real = [0];
  this._queryCache[id].virtual = [0];
  
  this._qrList.push(id);
  this._queryCount += 1;
  return this._queryCache[id];
};

OffsetCache.prototype.getId = function(query) {
  var subject = query.subject || '',
      predicate = query.predicate || '',
      object = query.object || '',
      id = subject+ ' '+ object+ ' '+ predicate;
  return id;
};

OffsetCache.prototype.addQueryLength = function(query,length) {
  var id = this.getId(query);
  if(this._queryCache[id] === undefined) {
    this._initQuery(query);
  }
  this._queryCache[id].length = length;
  return ;
};

OffsetCache.prototype.getLength = function(query) {
  var id = this.getId(query);
  if(this._queryCache[id] === undefined || this._queryCache[id].length === undefined) {
    return undefined;
  }
  return this._queryCache[id].length;
};

OffsetCache.prototype.addToCache = function(query,virtual,real) {
  var id = this.getId(query);
  if(this._queryCache[id] === undefined) {
    this._initQuery(query);
  }
  this._cleanupOffset(id);
  var virtualOffList = this._queryCache[id].virtual,
      realOffList = this._queryCache[id].real,
      iterations = virtualOffList.length,
      marked = false;
  for(var i = 0; i < iterations; i++) {
    if(virtualOffList[i] == virtual || realOffList[i] == real) {
      if(real > realOffList[i]) { // We have a new, further real offset
        realOffList[i] = real;
      }
      marked = true; // This is already in the cache
      break;
    }
    if(virtualOffList[i] < virtual) continue;
    // At this point, we know that we can insert in the current position
    virtualOffList.splice(i,0,virtual);
    realOffList.splice(i,0,real);
    marked = true;
    break;
  }
  if(!marked) {
    virtualOffList.push(virtual);
    realOffList.push(real);
  }
  return ;
};

OffsetCache.prototype.getClosestLowerOffset = function(query,offset) {
  offset = offset || query.offset;
  var id = this.getId(query);
  if(this._queryCache[id] === undefined) {
    this._initQuery(query);
  }
  var virtualOffList = this._queryCache[id].virtual,
      realOffList = this._queryCache[id].real,
      iterations = virtualOffList.length,
      base;
  
  for(var i = 0; i < iterations; i++) {
      if(virtualOffList[i] <= offset) base = i;
      else break;
  }
  return {virtual:virtualOffList[base], real:realOffList[base]};
};

module.exports = OffsetCache;
