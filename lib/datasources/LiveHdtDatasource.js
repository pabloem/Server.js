/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

/** A LiveHdtDatasource loads and queries an HDT document in-process. It also keeps track of live updates. */

var Datasource = require('./Datasource'),
    fs = require('fs'),
    hdt = require('hdt'),
    PollingAgent = require('./live/PollingAgent.js'),
    levelup = require('levelup'),
    levelgraph = require('levelgraph');

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

  // Create polling agent, and changeset manager
  options.applyCsetCallback = this.applyOperationList.bind(this);
  this._pollingAgent = new PollingAgent(options);

  // Initialize the auxiliary datastores
  this._initializeAuxiliaryStores();

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
LiveHdtDatasource.prototype._initializeAuxiliaryStores = function() {
  var add_path = this._workspace+'added.db',
      remove_path = this._workspace+'removed.db';
  this._auxiliary = {added: levelgraph(levelup(add_path)),
                     removed: levelgraph(levelup(remove_path))};
};

// Removes from the tripleStore and the tripleList all triples that are contained in both of them
LiveHdtDatasource.prototype._removeIntersections = function(tripleStore, tripleList,callback) {
    var ids = [];
    for(var i = 0; i < tripleList.length; i++) {
        var tr = tripleList[i],
            indx = i;
        console.log("PRE: Triple, i: "+JSON.stringify(tr)+", "+i);
        tripleStore.get(tr, function(err, list) {
            console.log("RES: Triple, i: "+JSON.stringify(tr)+", "+i);
            if(list.length > 0) {
                tripleStore.del(tr);
                ids.push(indx);
            }
            if(indx == tripleList.length-1) { // This is the last triple we look up
                for(var j = ids.length -1; j >= 0; j--) {
                    tripleList.splice(ids[j],1);
                    if(callback) callback();
                }
            }
        });
    }
};

LiveHdtDatasource.prototype._addAll = function(tripleStore, tripleList) {
    for(var i = 0; i < tripleList.length; i++) {
        var tr = tripleList[i],
            idx = i;
        tripleStore.put(tr, 
                        function(err) { 
                            if(err) console.log("Error: "+err+" | TR: "+tr);
                            if(idx == tripleList.length - 1) ; // A conclusion function, if there's any
                        });
    }
};

LiveHdtDatasource.prototype.applyOperationList = function(opList,callback) {
    this._removeIntersections(this._auxiliary.added, opList.removed);
    this._removeIntersections(this._auxiliary.removed, opList.added);
    this._addAll(this._auxiliary.added, opList.added);
    this._addAll(this._auxiliary.removed, opList.removed);
    if(callback) callback();
};

LiveHdtDatasource.prototype.startup = function() {
    if(this._started) return;
    this._started = true;
    this._pollingAgent.startPolling();
};

// Writes the results of the query to the given triple stream
LiveHdtDatasource.prototype._executeQuery = function (query, tripleStream, metadataCallback) {
  if(!this._started)
    this.startup();
  this._hdtDocument.search(query.subject, query.predicate, query.object,
                           { limit: query.limit, offset: query.offset },
    function (error, triples, estimatedTotalCount) {
      if (error) return tripleStream.emit('error', error);
      // Ensure the estimated total count is as least as large as the number of triples
      var tripleCount = triples.length, offset = query.offset || 0;
      if (tripleCount && estimatedTotalCount < offset + tripleCount)
        estimatedTotalCount = offset + (tripleCount < query.limit ? tripleCount : 2 * tripleCount);
      metadataCallback({ totalCount: estimatedTotalCount });
      // Add the triples to the stream
      for (var i = 0; i < tripleCount; i++)
        tripleStream.push(triples[i]);
      tripleStream.push(null);
    });
};

// Closes the data source
LiveHdtDatasource.prototype.close = function (done) {
  this._started = false;
  this._pollingAgent.stopPolling();
  this._hdtDocument.close(done);
};

module.exports = LiveHdtDatasource;
