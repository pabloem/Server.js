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
    path = require('path'),
    ChildProcess = require('child_process'),
    Bloem = require('bloem').Bloem;

var hdtFileLocation = 'hdtFileLocation.txt',
    addedDbLocation = 'addedDbLocation.txt',
    removedDbLocation = 'removedDbLocation.txt';

// Creates a new HdtDatasource
function LiveHdtDatasource(options) {
  if (!(this instanceof LiveHdtDatasource))
    return new LiveHdtDatasource(options);
  Datasource.call(this, options);

  options = options || {};

  // Adding our working directory and regenerating script
  this._workspace = options.workspace || 'workspace/';
  this._regenerator = options.regeneratorScript || './consolidate.sh';

  // Test whether the HDT file exists
  var hdtFile = (options.file || '').replace(/^file:\/\//, '');
  if (!fs.existsSync(hdtFile) || !/\.hdt$/.test(hdtFile))
    throw Error('Not an HDT file: ' + hdtFile);

  // Create polling agent, and changeset manager within it
  this._pollingAgent = new PollingAgent(options,this.applyOperationList.bind(this),this.rebuildHdtDatabase.bind(this));

  var _this = this,
      afterAux = function(){ _this._pollingAgent.startPolling(); },
      added = this._getAuxDbFile(options.addedTriplesDb,'added'),
      removed = this._getAuxDbFile(options.removedTriplesDb,'removed');
  // Initialize the auxiliary datastores
  this._initializeAuxiliaryStores(added,removed,afterAux);
  
  hdtFile = this._getHdtFile(hdtFile);
  this.loadHdtDocument(hdtFile);
}
Datasource.extend(LiveHdtDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

LiveHdtDatasource.prototype._getAuxDbFile = function(inputFile,type) {
  var locatorFile = type == 'added' ? addedDbLocation : removedDbLocation,
      dbLocationPath = path.join(this._workspace,locatorFile);
  if(fs.existsSync(dbLocationPath)) {
    return fs.readFileSync(dbLocationPath).toString();
  }
  if(inputFile === undefined) return type+'.db';
  return inputFile;
};

LiveHdtDatasource.prototype._getHdtFile = function(hdtFile) {
  var hdtLocationPath = path.join(this._workspace,hdtFileLocation);
  if(fs.existsSync(hdtLocationPath)) {
    return fs.readFileSync(hdtLocationPath).toString();
  }
  return hdtFile;
};

LiveHdtDatasource.prototype.loadHdtDocument = function(hdtFile) {
  // Store requested operations until the HDT document is loaded
  var hdtPath = hdtFile[0] == '/' ? hdtFile : path.join(process.cwd(),hdtFile);
  this._auxiliaryPaths = this._auxiliaryPaths || {};
  this._auxiliaryPaths.hdt = hdtPath;
  var _this = this, pendingCloses = [], pendingSearches = [];
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
};

LiveHdtDatasource.prototype.rebuildHdtDatabase = function(callback) {
  var _this = this,
      time = new Date(),
      hdtFileName = 'hdtbuild'+time.getFullYear()+''+time.getMonth()+''+time.getDate()+''+time.getHours()+'.hdt',
      addedFileName = 'added'+time.getFullYear()+''+time.getMonth()+''+time.getDate()+''+time.getHours(),
      removedFileName = 'removed'+time.getFullYear()+''+time.getMonth()+''+time.getDate()+''+time.getHours();
  var newHdt = path.join(this._workspace,hdtFileName),
      newAdded = path.join(this._workspace,addedFileName),
      newRemoved = path.join(this._workspace,removedFileName),
      rebuilder = ChildProcess.spawn(this._regenerator,[this._auxiliaryPaths.added, this._auxiliaryPaths.removed, 
                                                        this._auxiliaryPaths.hdt, newHdt]);
  var oldHdt = this._auxiliaryPaths.hdt,
      oldAdded = this._auxiliaryPaths.added,
      oldRemoved = this._auxiliaryPaths.removed;
  console.log("Regenerating database.");
  var callbackCount = 0,
      callbackCounter = function() {
        callbackCount += 1;
        if(callbackCount == 2 && callback) callback();
      };
  rebuilder.on('exit',function(code) {
    if(code !== 0) return;
    _this._auxiliaryPaths.hdt = newHdt;
    _this.initializeAuxiliaryStores(newAdded,newRemoved,callbackCounter);
    _this.loadHdtDocument(newHdt);
    var dbCleaner = ChildProcess.spawn("rm",["-r",oldHdt,oldAdded,oldRemoved]);
    dbCleaner.on('exit',function(code) {
      fs.writeFile(hdtFileLocation,newHdt,function(e) { if(e) throw e; });
      _this._auxiliaryPaths.hdt = newHdt;
      _this.loadHdtDocument(newHdt);
      callbackCounter();
    });
  });
};

// Opens and initializes the auxiliary triple stores
LiveHdtDatasource.prototype._initializeAuxiliaryStores = function(addedTriplesDb,removedTriplesDb,callback) {
  var added_file = addedTriplesDb,
      removed_file = removedTriplesDb;
  var add_path = added_file[0] == '/' ? added_file : path.join(this._workspace,(added_file)),
      remove_path = removed_file[0] == '/' ? removed_file : path.join(this._workspace,(removed_file));
  this._auxiliaryPaths = this._auxiliaryPaths || {};
  this._auxiliaryPaths.added = add_path;
  this._auxiliaryPaths.removed = remove_path;

  this._auxiliary = this._auxiliary || {};
  this._auxiliary.added = levelgraph(levelup(add_path));
  this._auxiliary.removed = levelgraph(levelup(remove_path));
  this._auxiliary.removedBloom = new Bloem(1674918,2); // Size matching a SafeBloem(500 000, 0.2)
  this._offsetCache = this._offsetCache || new OffsetCache();
  this._offsetCache.flushCache();

  /* Gotta fill up the Bloom filter with previously removed triples */
  var _this = this,
      removedStream = this._auxiliary.removed.getStream({});
  removedStream.on('data',function(data) {
    var id = data.subject+' '+data.predicate+' '+data.object;
    _this._auxiliary.removedBloom.add(id);
  });
  removedStream.on('end',function() {
    // TODO - before this point, queries may fail.
    if(callback) callback();
  });
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

LiveHdtDatasource.prototype._addAll = function(tripleStore, tripleList,callback) {
    tripleStore.put(tripleList, 
                    function(err) { 
                      if(err) console.log("Error: "+err);
                      callback();
                    });
};

LiveHdtDatasource.prototype._addToBloom = function(bloom, tripleList) {
    for(var i=0; i < tripleList.length; i++) {
        var tr = tripleList[i],
            id = tr.subject+' '+tr.predicate+' '+tr.object;
        bloom.add(Buffer(id));
    }
    return ;
};

LiveHdtDatasource.prototype._copyBeforeApply = function(newAdded,newRemoved,callback) {
  var exitCount = 0,
      exitCounter = function(code){
        exitCount += 1;
        if(exitCount != 2) return;
        console.log("Done removing LOCKS!");
        if(callback) callback();
      },
      add_copier = ChildProcess.spawn("cp",["-r",this._auxiliaryPaths.added, newAdded]),
      rmv_copier = ChildProcess.spawn("cp",["-r",this._auxiliaryPaths.removed, newRemoved]);
  
  var removeCount = 0,
      removeCounter = function(code) {
        // We have to remove the locks that were also copied
        removeCount += 1;
        if(removeCount != 2) return;
        console.log("Done copying!");
        var add_remover = ChildProcess.spawn("rm",['-f',path.join(newRemoved,"LOCK")]),
            rmv_remover = ChildProcess.spawn("rm",['-f',path.join(newRemoved,"LOCK")]);
        add_remover.on('exit',exitCounter);
        rmv_remover.on('exit',exitCounter);
      };
  add_copier.on('exit',removeCounter);
  rmv_copier.on('exit',removeCounter);
};

LiveHdtDatasource.prototype._swapAfterApply = function(addedDb,addedPath,removedDb,removedPath,callback) {
  var oldAddedPath = this._auxiliaryPaths.added,
      oldRemovedPath = this._auxiliaryPaths.removed;
  fs.writeFile(path.join(this._workspace,removedDbLocation),removedPath,function(e){if(e) throw(e);});
  fs.writeFile(path.join(this._workspace,addedDbLocation),addedPath,function(e){if(e) throw(e);});
  this._auxiliary.added = addedDb;
  this._auxiliary.removed = removedDb;
  this._auxiliaryPaths.added = addedPath;
  this._auxiliaryPaths.removed = removedPath;
  ChildProcess.spawn("rm",["-rf",oldAddedPath]);
  ChildProcess.spawn("rm",["-rf",oldRemovedPath]);
  console.log("Swapped databases!");
  if(callback) callback();
};

LiveHdtDatasource.prototype.applyOperationList = function(opList,callback) {
  var _this = this,
      t = new Date(),
      addedFileName = 'added'+t.getFullYear()+''+t.getMonth()+''+t.getDate()+''+t.getHours()+''+t.getMinutes()+''+t.getMilliseconds(),
      removedFileName = 'removed'+t.getFullYear()+''+t.getMonth()+''+t.getDate()+''+t.getHours()+''+t.getMinutes()+''+t.getMilliseconds();
  var newAdded = path.join(this._workspace,addedFileName),
      newRemoved = path.join(this._workspace,removedFileName),
      newAddedDb, newRemovedDb;

  /* The following two lines are in case we need to update databases in the same minute, which is unlikely outside of testing
  if(newAdded == this._auxiliaryPaths.added) newAdded = newAdded+'_1';
  if(newRemoved == this._auxiliaryPaths.removed) newRemoved = newAdded+'_1';*/
  
  console.log("Applying opList: Rmv-"+opList.removed.length+" | Add-"+opList.added.length);
  var removeCount = 0,
      callbackCount = 0,
      callbackCounter = function() {
        callbackCount += 1;
        if(callbackCount == 2) {
          console.log("Done applying!");
          _this._swapAfterApply(newAddedDb,newAdded,newRemovedDb,newRemoved,callback);
        }
      },
      afterCopy = function() {
        _this._addToBloom(_this._auxiliary.removedBloom, opList.removed);
        _this._offsetCache.flushCache(); // We need to flush our cache, because we have updates
        newAddedDb = levelgraph(levelup(newAdded));
        newRemovedDb = levelgraph(levelup(newRemoved));
        _this._addAll(newAddedDb, opList.added,function() {
          callbackCounter();
          _this._addAll(newRemovedDb, opList.removed,callbackCounter);
        });
      },
      countRemove = function() {
        removeCount += 1;
        if(removeCount == 2) {
          _this._copyBeforeApply(newAdded,newRemoved,afterCopy);
        }
  };
  this._removeIntersections(this._auxiliary.added, opList.removed, countRemove);
  this._removeIntersections(this._auxiliary.removed, opList.added, countRemove);
};

LiveHdtDatasource.prototype._getAddedTriples = function(statusDic, query, tripleStream, metadataCallback) {
  var _this = this,
      finalizeCount = 0,
      finalizeCounter = function() {
        finalizeCount += 1;
        if(finalizeCount != 2) return;
        tripleStream.push(null);
      };
  if(statusDic.servedTriples == query.limit) {
    this._returnEstTotalCount(query,statusDic,metadataCallback,finalizeCounter);
    tripleStream.push(null);    
    return;
  }
  this._auxiliary.added.get({subject:query.subject, predicate: query.predicate, object:query.object,
                             limit: statusDic.newLimit, offset: statusDic.newOffset},
                            function(error,list) {
                              if (error) return tripleStream.emit('error', error);
                              statusDic.servedTriples += list.length;
                              _this._returnEstTotalCount(query,statusDic,metadataCallback,finalizeCounter);
                              for(var i=0; i < list.length; i++) {
                                tripleStream.push(list[i]);
                              }
                              finalizeCounter();
                            });
};

/* We use the Chernoff bound to calculate a limit to ensure we fetch all necessary triples
   on the first trial with 99.9% probability.
   We model whether a triple has been removed as  a random variable 'ti', that is 0 if a
   triple with index i has been removed, and 1 otherwise,

   See Issue #7 for the explanation of the limit calculation through Chernoff bound.
*/
LiveHdtDatasource.prototype._probabilisticGetLimit = function(limit,extraOffset) {
  if(!limit) return undefined;
  var HDTCount = 300000000.0, // 3x10^8
      REMCount = 300000.0,    // 3x10^5
      PrRemoved = REMCount/HDTCount, // Probability of a triple being removed. 3x10^-3
      PrNotRemoved = (HDTCount-REMCount)/HDTCount, // Prob of a triple not being removed
      EV_Exp_singleTr = PrNotRemoved*Math.exp(-1)+PrRemoved, // Expected value of exp(ti)
      PrFail = 1 - 0.999;     // Probability of not fetching enough triples. 1 - Prob of success
  var N = Math.ceil((Math.log(PrFail) - limit)/Math.log(EV_Exp_singleTr));
  return N;
};

LiveHdtDatasource.prototype._computeHdtRemoved = function(countDic,triples,removedStore,idx,callback) {
  /* Look on the Bloom filter first.*/
  var tr = triples[idx],
      id = tr.subject+' '+tr.predicate+' '+tr.object,
      finalize = function() {
        countDic.got += 1;
        if(countDic.got == countDic.waiting) {
          callback();
        }
      };

  if(!this._auxiliary.removedBloom.has(id)) {
    finalize();
    return ;
  }
  /* If Bloom filter says MAY BE, we look in the datastore */
  this._auxiliary.removed.get(tr,function(e,l) {
    if(l.length > 0) removedStore.addTriple(tr.subject,tr.predicate,tr.object);
    finalize();
  });
};

// Runs a rough estimate of how many triples match the query
LiveHdtDatasource.prototype._returnEstTotalCount = function(query,statusDic,metadataCallback,callback) {
  var hdtCount = statusDic.counts.hdt,
      queryOffset = query.offset || 0,
      queryLimit = query.limit || 0,
      servedTriples = statusDic.servedTriples,
      counter = 0,
      returnCount = function() {
        if(counter != 2) return;
        var res = hdtCount + statusDic.counts.added - statusDic.counts.removed;
        //console.log("Res: "+res+" | HDT: "+hdtCount+" | Add: "+statusDic.counts.added+" | Remv: "+statusDic.counts.removed+
        //" | Servd: "+statusDic.servedTriples+ " | Lim: "+queryLimit+" | Off: "+queryOffset);
        if(res < statusDic.servedTriples + queryOffset)
          res = queryOffset + (!query.limit || servedTriples < queryLimit ? servedTriples : servedTriples*2);
        if(metadataCallback) metadataCallback({totalCount: res});
        callback();
      };
  this._auxiliary.added.approximateSize(query,function(e,sz) {
    statusDic.counts.added = sz;
    counter += 1;
    returnCount();
  });
  this._auxiliary.removed.approximateSize(query,function(e,sz) {
    statusDic.counts.removed = sz;
    counter += 1;
    returnCount();
  });
};

// Does the necessary actions after looking up triples in an HDT document
LiveHdtDatasource.prototype._afterHdtTriples = function(statusDic,query,tripleStream,metadataCallback,
                                                        limit, tripleCount) {
  /*Case 0.1: We have not served any triples, nor have we reached the end of HDT file
         Conditions - tripleCount == limit and servedTriples == 0
    Case 0.2: We have served some triples, and have not reached the end of HDT file
         Conditions - tripleCount == limit and servedTriples > 0
   */
  if(limit && tripleCount == limit && statusDic.servedTriples < query.limit) { // Case 0
    if(statusDic.servedTriples === 0) { // Case 0.1
    }
    if(statusDic.servedTriples > 0) { // Case 0.2
    }
    this._getHdtTriples(statusDic,query,tripleStream,metadataCallback);
    return ;
  }
  var newLimit, newOffset;
  /*Case 1: We have not served any triples, and have reached the end of HDT file
        Conditions - servedTriples == 0 and tripleCount < limit
        New offset = query.offset - (statusDic.virtualOffset + statusDic.virtualTriples)
        New limit  = query.limit */
  if(statusDic.servedTriples === 0 && tripleCount < limit) {
    newLimit = query.limit;
    newOffset = query.offset - (statusDic.virtualOffset + statusDic.virtualTriples);
  }
  /*Case 2: We have served some triples, but not all - and reached the end of HDT file
        Conditions - servedTriples > 0 and servedTriples < query.limit and triplecount < limit
        New offset = 0
        New limit  = query.limit - statusDic.servedTriples */
  if(statusDic.servedTriples > 0 && statusDic.servedTriples < query.limit && tripleCount < limit) {
    newOffset = 0;
    newLimit = query.limit - statusDic.servedTriples;
  }
  /*Case 3: We have served all triples already
        Conditions - servedTriples == query.limit
        No calculations necessary */
  if(statusDic.servedTriples == query.limit) {
  }

  statusDic.newLimit = newLimit;
  statusDic.newOffset = newOffset;
  this._getAddedTriples(statusDic,query,tripleStream,metadataCallback);
  return ;
};
                                                        

LiveHdtDatasource.prototype._getHdtTriples = function(statusDic, query,tripleStream,metadataCallback) {
  var _this = this,
      pendingOffset = query.offset+statusDic.servedTriples,
      offDic = this._offsetCache.getClosestLowerOffset(query,pendingOffset),
      limit = this._probabilisticGetLimit(query.limit,query.offset - offDic.virtual),
      realOffset = offDic.real,
      virtualOffset = offDic.virtual;
  statusDic.virtualOffset = virtualOffset || 0;
  statusDic.virtualTriples = 0;
  this._hdtDocument.search(query.subject, query.predicate, query.object,
                           { limit: limit, offset: realOffset },
    function (error, triples, estimatedTotalCount) {
      if (error) return tripleStream.emit('error', error);
      // Ensure the estimated total count is as least as large as the number of triples
      var tripleCount = triples.length, offset = query.offset || 0,
          remStore = N3.Store();
      statusDic.counts.hdt = estimatedTotalCount;
      var countDic = {got:0,waiting:tripleCount};
      for(var i = 0; i < tripleCount; i++) {
        _this._computeHdtRemoved(countDic,triples,remStore,i,function(){
          var counter = virtualOffset || 0;
          for(var i = 0; i < tripleCount; i++) {
            // If this triple is among the removed triples, then we skip it
            if(remStore.find(triples[i].subject,triples[i].predicate,triples[i].object).length > 0) continue;
            // virtualTriples keeps track of how many non-removed triples we've observed
            statusDic.virtualTriples += 1;
            // The following is to keep the offset cache fresh with the offsets we are calculating
            // We add a check on the size of limit, to avoid flooding the Cache with small intervals
            if(query.limit && query.limit >= 10 && 
               (counter % query.limit === 0)) _this._offsetCache.addToCache(query,counter,realOffset+i);
            
            // We shall serve the triple if the following two conditions are true:
            // 1. The offset condition
            //    1.1 - We have no offset or,
            //    1.2 - We are past the offset
            // 2. The limit condition
            //    2.1 - We have no limit or,
            //    2.2 - We are before the limit
            if((!offset || counter - offset >= statusDic.servedTriples) && (!query.limit || statusDic.servedTriples < query.limit)) {
              statusDic.servedTriples += 1;
              tripleStream.push(triples[i]);
            }
            /* If we have a limit, and we reached it, then we break out */
            if(query.limit && statusDic.servedTriples == query.limit) break;
            // "COUNTER" Counts the number of valid triples that we've observed
            counter += 1;
          }
          //console.log("Counter: "+counter+" | served: "+statusDic.servedTriples+" | i: "+i+" | Lim: "+limit+
          //" | OffDic: "+JSON.stringify(offDic)+" | NewOffDic: "+JSON.stringify({virtual:counter,real:realOffset+i}));
          _this._offsetCache.addToCache(query,counter,realOffset+i);
          _this._afterHdtTriples(statusDic,query,tripleStream,metadataCallback,limit,tripleCount);
        });
      }
      if(!tripleCount) _this._afterHdtTriples(statusDic,query,tripleStream,metadataCallback,limit,tripleCount,realOffset);
    });
};

// Writes the results of the query to the given triple stream
LiveHdtDatasource.prototype._executeQuery = function (query, tripleStream, metadataCallback) {
  query.offset = query.offset || 0;
  var statusDic = {servedTriples:0};
  statusDic.counts = {};
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
  this._pollingAgent.stopPolling();
  this._hdtDocument.close(doneCount);
  this._auxiliary.added.close(doneCount);
  this._auxiliary.removed.close(doneCount);
};

module.exports = LiveHdtDatasource;
