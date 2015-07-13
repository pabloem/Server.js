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
  var add_path = this._workspace+(options.addedTriplesDb || 'added.db'),
      remove_path = this._workspace+(options.removedTriplesDb || 'removed.db');
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

LiveHdtDatasource.prototype._serveQueryResults = function(query,resDic,resDicCount,tripleStream,metadataCallback) { 
  var hdtList = resDic.hdt,
      hdtCount = resDic.hdt ? resDic.hdt.length : 0;

  var totalCount = resDicCount.hdt + resDicCount.added,
      countEstimate = resDic.hdt.length + resDic.added.length + query.offset;
  if(totalCount < countEstimate) totalCount = countEstimate;
  metadataCallback({ totalCount: totalCount });
  for(var i = 0; i < hdtCount; i++) {
    tripleStream.push(hdtList[i]);
  }
  var addedCount = resDic.added.length,
      addedList = resDic.added;
  for(var j = 0; j < addedCount; j++) {
    tripleStream.push(addedList[j]);
  }
  tripleStream.push(null);
};

LiveHdtDatasource.prototype._queryAuxAdded = function(query, resDic, resDicCount,
                                                      tripleStream, metadataCallback) {
  if(query.offset+query.limit < resDic.estimatedTotalCount) {
    resDicCount.added = 0;
    resDic.added = [];
    this._serveQueryResults(query,resDic,resDicCount,tripleStream,metadataCallback);
    return;
  }
  var _this = this,
      startPoint = query.offset - resDicCount.hdt, limit;
  if(query.limit !== undefined && query.limit !== 0) {
    limit = query.limit + (startPoint < 0 ? startPoint : 0);
  }
  if(startPoint < 0) startPoint = 0;
  this._auxiliary.added.get({subject:query.subject, predicate: query.predicate, object:query.object,
                             limit: limit, offset: startPoint},
                           function(error,list) {
                             if (error) return tripleStream.emit('error', error);
                             resDic.added = list;
                             resDicCount.added = list.length;
                             _this._serveQueryResults(query,resDic,resDicCount,tripleStream,metadataCallback);
                           });
};

// Writes the results of the query to the given triple stream
LiveHdtDatasource.prototype._executeQuery = function (query, tripleStream, metadataCallback) {

  query.offset = query.offset || 0;
  var resDic = {},
      resDicCount = {},
      _this = this;

  this._hdtDocument.search(query.subject, query.predicate, query.object,
                           { limit: query.limit, offset: query.offset },
    function (error, triples, estimatedTotalCount) {
      if (error) return tripleStream.emit('error', error);
      // Ensure the estimated total count is as least as large as the number of triples
      var tripleCount = triples.length, offset = query.offset || 0;
      if (tripleCount && estimatedTotalCount < offset + tripleCount)
        estimatedTotalCount = offset + (tripleCount < query.limit ? tripleCount : 2 * tripleCount);
      resDicCount.hdt = estimatedTotalCount;
      // Add the triples to the dictionary
      resDic.hdt = triples;
      _this._queryAuxAdded(query, resDic, resDicCount, tripleStream, metadataCallback);
    });
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
