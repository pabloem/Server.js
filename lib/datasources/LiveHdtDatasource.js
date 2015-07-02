/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

/** A LiveHdtDatasource loads and queries an HDT document in-process. It also keeps track of live updates. */

var Datasource = require('./Datasource'),
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
    this._removeIntersections(this._auxiliary.added, opList.removed, countRemove);
    this._removeIntersections(this._auxiliary.removed, opList.added, countRemove);
};

LiveHdtDatasource.prototype.startup = function() {
    if(this._started) return;
    this._started = true;
    this._pollingAgent.startPolling();
};

LiveHdtDatasource.prototype.largestTriple = function(first,second) {
    if(!first || !second) return first;
    if((first.subject > second.subject) ||
       (first.subject == second.subject && 
        first.predicate > second.predicate) || 
       (first.subject == second.subject &&
        first.predicate == second.predicate &&
        first.object >= second.object)) return first;
    return second;
};

LiveHdtDatasource.prototype._checkQueryResult = function(resCount, resDic, resDicCount,
                                                         tripleStream, metadataCallback, query) {
    if(resCount < 3) return ;
  /* We calculate new limits and offsets for the query. We do this to know how many of the
   new added triples to include in the results.
   When we present the 'limit' number of triples coming from the HDT file (these are triples 
   'offset', 'offset+1',..,'offset+limit-1'), we append after them all triples in the 
   'added' auxiliary dataset that come AFTER triples 'offset' and BEFORE 'offset+limit'.

   Note that triple 'offset+limit' is actually the first triple of the next page, and 
   thus this triple must not be shown, but it should be retrieved from the HDT 
   document, to calculate which triples in the 'added' auxiliary dataset must be appended
   to the result.

   Also, when the offset is 0, then the query retrieves triples '0', '1',.., 'limit-1'. In
   this case, we also have to add all triples in the 'added' auxiliary dataset that come
   BEFORE triple '0'.

   Given the previous explanation, here's how we calculate the 'limit' and 'offset' variables:


   = query.offset is 0 or undefined           ============
      In this case, the query has offset 0. This means that
      we must add any triples that come BEFORE triple '0' in the HDT file. This is a very
      common corner case. In this case, as usual, we retrieve the first triple of the next
      page, to 
   = query.limit is undefined or 0            ============
      In this case, there is no limit for the query. We should just add ALL the results coming
      from the 'added' auxiliary database.
   = query.limit is an integer (not 0, nor undefined)  ===
      In this case, we get ONE MORE element than the limit of the query (i.e. we retrieve elements
      'offset' to 'offset+limit-1' and also retrieve 'offset+limit').
   */
    var rmStore = N3.Store(),
        resultStore = N3.Store(),
        removed = resDic.removed,
        added = resDic.added,
        main = resDic.hdt,
        limit = (query.limit === undefined || query.limit === 0) ? 0 : 1;

    for(var i = 0; i < removed.length; i++) {
        rmStore.addTriple(removed[i].subject,removed[i].predicate, removed[i].object);
    }
    var dif = main.length - limit;
    for(i = 0; i < dif; i++) {
        var fnd = rmStore.find(main[i].subject,main[i].predicate,main[i].object);
        if(fnd.length === 0) {
            resultStore.addTriple(main[i].subject,main[i].predicate,main[i].object);
            continue;
        }
    }
    var firstTriple = main[0],
        maxTriple = (query.limit === 0 || query.limit === undefined) ? undefined : main[main.length - 1],
        addedCount = added.length;

    for(var j = 0; j < addedCount; j++) {
        //console.log("Largest added[i]/first: " + (added[j] == this.largestTriple(added[j],firstTriple) ? "added" : "first"));
        //console.log("Largest max/added[i]: " + (maxTriple == this.largestTriple(maxTriple,added[j]) ? "max" : "added"));
        if(added[j] == this.largestTriple(added[j],firstTriple) &&
           maxTriple == this.largestTriple(maxTriple,added[j])) {
            // We send all triples in between the first triple, and the max triple 
            // - which is (the first result of next pg)
            resultStore.addTriple(added[j].subject,added[j].predicate,added[j].object);
        } else if (firstTriple == this.largestTriple(added[j],firstTriple) &&
                   (query.offset === 0 || query.offset === undefined)) {
            // If there is no offset, then we add all triples that come before the first triple too.
            resultStore.addTriple(added[j].subject,added[j].predicate,added[j].object);
        }
    }
    var results = resultStore.find();
    for(i = 0; i < results.length; i++) {
        tripleStream.push(results[i]);
    }
    tripleStream.push(null);
};

// Writes the results of the query to the given triple stream
LiveHdtDatasource.prototype._executeQuery = function (query, tripleStream, metadataCallback) {
  if(!this._started) // TODO - This might not be a necessary check.
    this.startup();

  var resCount = 0,
      resDic = {},
      resDicCount = {},
      _this = this;

  var limit = query.limit;
  if(query.limit !== undefined && query.limit !== 0) {
      // We add one extra triple to the result. See explanation on _checkQueryResult
      limit += 1;
  }

  this._hdtDocument.search(query.subject, query.predicate, query.object,
                           { limit: limit, offset: query.offset },
    function (error, triples, estimatedTotalCount) {
      resCount += 1;
      if (error) return tripleStream.emit('error', error);
      // Ensure the estimated total count is as least as large as the number of triples
      var tripleCount = triples.length, offset = query.offset || 0;
      if (tripleCount && estimatedTotalCount < offset + tripleCount)
        estimatedTotalCount = offset + (tripleCount < query.limit ? tripleCount : 2 * tripleCount);
      //metadataCallback({ totalCount: estimatedTotalCount });
      resDicCount.hdt = estimatedTotalCount;
      // Add the triples to the dictionary
      resDic.hdt = triples;
      _this._checkQueryResult(resCount, resDic, resDicCount, 
                              tripleStream, metadataCallback, query);
    });
  this._auxiliary.added.get({subject:query.subject, predicate:query.predicate,
                             object: query.object},
                           function(err,lst) {
                             resCount += 1;
                             if (err) return tripleStream.emit('error',err);
                             resDic.added = lst;
                             resDicCount.added = lst.length;
                             _this._checkQueryResult(resCount, resDic, resDicCount, 
                                                     tripleStream, metadataCallback,query);
                           });
  this._auxiliary.removed.get({subject:query.subject, predicate:query.predicate,
                             object: query.object},
                           function(err,lst) {
                             resCount += 1;
                             if (err) return tripleStream.emit('error',err);
                             resDic.removed = lst;
                             resDicCount.removed = lst.length;
                             _this._checkQueryResult(resCount, resDic, resDicCount, 
                                                     tripleStream, metadataCallback,query);
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
