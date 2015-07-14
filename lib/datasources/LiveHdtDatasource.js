/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

/** A LiveHdtDatasource loads and queries an HDT document in-process. It also keeps track of live updates. */

var Datasource = require('./Datasource'),
    OffsetCache = require('./live/OffsetCache.js'),
    fs = require('fs'),
    hdt = require('hdt'),
    PollingAgent = require('./live/PollingAgent.js'),
    levelup = require('levelup'),
    levelgraph = require('levelgraph'),
    N3 = require('n3'),
    path = require('path');

// Creates a new HdtDatasource
function LiveHdtDatasource(options) {
  if (!(this instanceof LiveHdtDatasource))
    return new LiveHdtDatasource(options);
  Datasource.call(this, options);

  options = options || {};

  // Adding our working directory
  this._workspace = options.workspace || 'workspace/';

  // Test whether the HDT file exists
  var hdtFile = (options.file || '').replace(/^file:\/\//, '');
  if (!fs.existsSync(hdtFile) || !/\.hdt$/.test(hdtFile))
    throw Error('Not an HDT file: ' + hdtFile);

  // Create polling agent, and changeset manager within it
  options.applyCsetCallback = this.applyOperationList.bind(this);
  this._pollingAgent = new PollingAgent(options);

  // Initialize the auxiliary datastores
  this._initializeAuxiliaryStores(options);

  // Store requested operations until the HDT document is loaded
  var pendingCloses = [], pendingSearches = [];
  this._hdtDocument = { close:  function () { pendingCloses.push(arguments); },
                        search: function () { pendingSearches.push(arguments); } };

  // Load the HDT document
  hdt.fromFile(hdtFile, function (error, hdtDocument) {
    // Set up an error document if the HDT document could not be opened
    this._hdtDocument = !error ? hdtDocument : hdtDocument = {
      close:  function (callback) { callback && callback(); },
      search: function (s, p, o, op, callback) { callback(error); },
    };
    // Execute pending operations
    pendingSearches.forEach(function (args) { hdtDocument.search.apply(hdtDocument, args); });
    pendingCloses.forEach(function (args) { hdtDocument.close.apply(hdtDocument, args); });
  }, this);
}
Datasource.extend(LiveHdtDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Opens and initializes the auxiliary triple stores
LiveHdtDatasource.prototype._initializeAuxiliaryStores = function(options) {
  var add_path = path.join(this._workspace,(options.addedTriplesDb || 'added.db')),
      remove_path = path.join(this._workspace,(options.removedTriplesDb || 'removed.db'));
  this._auxiliary = {added: levelgraph(levelup(add_path)),
                     removed: levelgraph(levelup(remove_path))};
  this._offsetCache = new OffsetCache();
};

// Removes from the tripleStore and the tripleList all triples that are contained in both of them
LiveHdtDatasource.prototype._removeIntersections = function(tripleStore, tripleList,callback) {
    tripleList = tripleList || [];
    var toRemove = N3.Store(),
        ret = [],
        searched = tripleList.length,
        got = 0;
    if(tripleList.length === 0) {
        // If the tripleList is a completely empty list, we just do the final
        // callback. Otherwise, we do the removals.
        callback();
        return ;
    }
    for(var i = 0; i < tripleList.length; i++) {
        var tr = tripleList[i];
        tripleStore.get(tr, function(err, list) {
            got += 1;
            if(list.length > 0) {
                tripleStore.del(list[0]);
                toRemove.addTriple(list[0].subject,list[0].predicate,list[0].object);
            }
            if(got == searched) {
                for(var i = tripleList.length-1; i >= 0; i--) {
                    if(toRemove.find(tripleList[i].subject,
                                     tripleList[i].predicate,
                                     tripleList[i].object)
                       .length > 0) {
                        tripleList.splice(i,1);
                    }
                }
                callback();
            }
        });
    }
};

LiveHdtDatasource.prototype._addAll = function(tripleStore, tripleList) {
    tripleStore.put(tripleList, 
                    function(err) { 
                        if(err) console.log("Error: "+err);
                    });
};

LiveHdtDatasource.prototype.applyOperationList = function(opList,callback) {
    var _this = this,
        count = 0,
        countRemove = function() {
            count += 1;
            if(count == 2) {
                _this._addAll(_this._auxiliary.added, opList.added);
                _this._addAll(_this._auxiliary.removed, opList.removed);
                callback();
            }
        };
    this._offsetCache.flushCache(); // We need to flush our cache, because we have updates
    this._removeIntersections(this._auxiliary.added, opList.removed, countRemove);
    this._removeIntersections(this._auxiliary.removed, opList.added, countRemove);
};

LiveHdtDatasource.prototype.startup = function() {
    if(this._started) return;
    this._started = true;
    this._pollingAgent.startPolling();
};

LiveHdtDatasource.prototype._getAddedTriples = function(statusDic, query, tripleStream, metadataCallback) {
  var _this = this,
      limit = query.limit - statusDic.servedTriples;
  limit = (limit < 0 ? 0 : limit);
  if(limit !== 0) {
    this._auxiliary.added.get({subject:query.subject, predicate: query.predicate, object:query.object,
                               limit: limit, offset: 0},
                              function(error,list) {
                                if (error) return tripleStream.emit('error', error);
                                for(var i=0; i < list.length; i++) {
                                  statusDic.servedTriples += 1;
                                  tripleStream.push(list[i]);
                                }
                                tripleStream.push(null);
                              });
  } else {
    tripleStream.push(null);
  }
};

LiveHdtDatasource.prototype._probabilisticGetLimit = function(limit,extraOffset) {
  if(!limit) return undefined;
  return 2*(limit+extraOffset);
};

LiveHdtDatasource.prototype._computeHdtRemoved = function(countDic,triples,removedStore,idx,callback) {
  /* Look on the Bloom filter first.
     If False: return;
     Else: */
  this._auxiliary.removed.get(triples[idx],function(e,l) {
    countDic.got += 1;
    if(l.length > 0) removedStore.addTriple(triples[idx].subject,triples[idx].predicate,triples[idx].object);
    if(countDic.got == countDic.waiting) {
      callback();
    }
  });
};

LiveHdtDatasource.prototype._returnEstTotalCount = function(query,statusDic,metadataCallback) {
  var hdtCount = statusDic.estimatedTotalCount,
      servedTriples = statusDic.servedTriples;
  this._auxiliary.removed.approximateSize(query,function(e,sz) {
    var res = hdtCount - sz;
    if(res < servedTriples) res = servedTriples;
    metadataCallback({totalCount: res});
  });
};

LiveHdtDatasource.prototype._afterHdtTriples = function(statusDic,query,tripleStream,metadataCallback,
                                                        limit, tripleCount) {
  // If we served all triples, or if we are at the end of the HDT file, we call _getAddedTriples - to finalize.
  if(query.limit == statusDic.servedTriples || !query.limit || tripleCount < limit) {
    this._returnEstTotalCount(query,statusDic,metadataCallback);
    this._getAddedTriples(statusDic,query,tripleStream,metadataCallback);
    // Add added triples (from the auxiliary store)
    return ;
  }
  // OTHERWISE, WE NEED TO QUERY AGAIN! - We do nothing - for now.
  console.log("Came here - Limit: "+limit+" | tripleCount: "+tripleCount+" | statusDic: "+JSON.stringify(statusDic));
  tripleStream.push(null);
  return ;
};
                                                        

LiveHdtDatasource.prototype._getHdtTriples = function(statusDic, query,tripleStream,metadataCallback) {
  var _this = this,
      offDic = this._offsetCache.getClosestLowerOffset(query,query.offset),
      limit = this._probabilisticGetLimit(query.limit,query.offset - offDic.virtual),
      realOffset = offDic.real,
      virtualOffset = offDic.virtual;
  this._hdtDocument.search(query.subject, query.predicate, query.object,
                           { limit: limit, offset: realOffset },
    function (error, triples, estimatedTotalCount) {
      if (error) return tripleStream.emit('error', error);
      // Ensure the estimated total count is as least as large as the number of triples
      var tripleCount = triples.length, offset = query.offset || 0,
          remStore = N3.Store();
      if (tripleCount && estimatedTotalCount < offset + tripleCount)
        estimatedTotalCount = offset + (tripleCount < query.limit ? tripleCount : 2 * tripleCount);
      statusDic.estimatedTotalCount = estimatedTotalCount;
      var countDic = {got:0,waiting:tripleCount};
      for(var i = 0; i < tripleCount; i++) {
        _this._computeHdtRemoved(countDic,triples,remStore,i,function(){
          var counter = virtualOffset || 0;
          for(var i = 0; i < tripleCount; i++) {
            // If this triple is among the removed triples, then we skip it
            if(remStore.find(triples[i].subject,triples[i].predicate,triples[i].object).length > 0) continue;
            // "COUNTER" Counts the number of valid triples
            counter += 1;
            // The following is to keep the offset cache fresh with the offsets we are calculating
            if(limit && counter % limit === 0) _this._offsetCache.addToCache(query,counter,realOffset+i);
            
            // We shall serve the triple if the following two conditions are true:
            // 1. The offset condition
            //    1.1 - We have no offset or,
            //    1.2 - We are past the offset
            // 2. The limit condition
            //    2.1 - We have no limit or,
            //    2.2 - We are before the limit
            if((!offset || counter >= query.offset) && (!query.limit || statusDic.servedTriples < query.limit)) {
              statusDic.servedTriples += 1;
              tripleStream.push(triples[i]);
            }
            /* If we have a limit, and we reached it, then we break out */
            if(query.limit && statusDic.servedTriples == query.limit) break;
          }
          _this._offsetCache.addToCache(query,counter,realOffset+i);
          _this._afterHdtTriples(statusDic,query,tripleStream,metadataCallback,limit,tripleCount);
        });
      }
      if(!tripleCount) _this._afterHdtTriples(statusDic,query,tripleStream,metadataCallback,limit,tripleCount);
    });
};

// Writes the results of the query to the given triple stream
LiveHdtDatasource.prototype._executeQuery = function (query, tripleStream, metadataCallback) {
  query.offset = query.offset || 0;
  var statusDic = {servedTriples:0};
  this._getHdtTriples(statusDic,query,tripleStream,metadataCallback);
};

// Closes the data source
LiveHdtDatasource.prototype.close = function (done) {
  var cnt = 0,
      doneCount = function() {
        cnt += 1;
        if(cnt == 3) {
          done();
        }
      };
  this._started = false;
  this._pollingAgent.stopPolling();
  this._hdtDocument.close(doneCount);
  this._auxiliary.added.close(doneCount);
  this._auxiliary.removed.close(doneCount);
};

module.exports = LiveHdtDatasource;
