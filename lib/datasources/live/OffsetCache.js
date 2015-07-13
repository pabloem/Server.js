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
  return Math.floor(Math.random() * (max - min + 1)) + min;
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
  var len = this._queryCache[id].label.length;
  if(len < this._offsetLimit) return;

  var target = this._getRandomInt(0,len);
  this._queryCache[id].label.splice(target,1);
  this._queryCache[id].offset.splice(target,1);
};

OffsetCache.prototype._initQuery = function(query) {
  this._cleanupQuery();
  var id = this.getId(query);
  this._queryCache[id] = {};
  this._queryCache[id].offset = [0];
  this._queryCache[id].label = [0];
  
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

OffsetCache.prototype.addToCache = function(query,label,offset) {
  var id = this.getId(query);
  if(this._queryCache[id] === undefined) {
    this._initQuery(query);
  }
  this._cleanupOffset(id);
  var labelList = this._queryCache[id].label,
      offsetList = this._queryCache[id].offset,
      iterations = labelList.length,
      marked = false;
  for(var i = 0; i < iterations; i++) {
    if(labelList[i] < label) continue;
    if(labelList[i] == label) {
      marked = true; // This is already in the cache
      break;
    }
    // At this point, we know that we can insert in the current position
    labelList.splice(i,0,label);
    offsetList.splice(i,0,offset);
    marked = true;
    break;
  }
  if(!marked) {
    labelList.push(label);
    offsetList.push(offset);
  }
  return ;
};

OffsetCache.prototype.getClosestLowerOffset = function(query,offset) {
  offset = query.offset || offset;
  var id = this.getId(query);
  if(this._queryCache[id] === undefined) {
    this._initQuery(query);
  }
  var labelList = this._queryCache[id].label,
      offsetList = this._queryCache[id].offset,
      iterations = labelList.length,
      base;
  
  for(var i = 0; i < iterations; i++) {
      if(labelList[i] <= offset) base = i;
      else break;
  }
  return {label:labelList[base], offset:offsetList[base]};
};

module.exports = OffsetCache;
