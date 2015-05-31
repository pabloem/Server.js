var request = require('request'),
    ChangesetCounter = require('./ChangesetCounter.js'),
    htmlparser = require('htmlparser2'),
    Changeset = require('./Changeset.js'),
    N3 = require('n3');

var CSM_WAITING_LISTS = "WAITING CSET LISTS",
    CSM_WAITING_CSETS = "WAITING CHANGESETS",
    CSM_READY = "READY";

function ChangesetManager(options) {
    options = options || {};
    this._base_url = options.baseUrl || 'http://live.dbpedia.org/changesets/';
    this._accepted_changesets = options.accepted_changesets || ['added','removed'];
    this._start_point = new ChangesetCounter(options.lastCset);
    this.status = CSM_READY;
    this._HOUR_STEP = options.hour_step || 20; // Maximum number of hours to check per cycle
    this._CSET_THRESHOLD = options.cset_threshold || 500; // Maximum number of changesets to apply per cycle

    /* We keep these function references to allow unit testing of each one of the
     steps in the querying process */
    this._afterListDownload = function(){};
    this._afterChangesetDownload = function(){};
}

/* Function: checkForChangesets
 This function is the main function, which runs the full check for new changesets,
 downloads them, and commits them to the database.
*/
ChangesetManager.prototype.checkForChangesets = function() {
    this._afterListDownload = this.downloadChangesets;
    this._afterChangesetDownload = this.applyAndCleanup;

    this.retrieveChangesetList(this._start_point);
};

/* This function is super ugly. Apologies. */
ChangesetManager.prototype._filterChangesetList = function(csets,minCsetCount,maxCsetCount) {
    var filteredCsets = [];
    for(var i = 0; i < csets.length; i ++) {
        if(parseInt(csets[i]) < minCsetCount) continue;
        if(maxCsetCount !== 0 && parseInt(csets[i]) >= maxCsetCount) continue;
        for(var j =0; j < this._accepted_changesets.length; j++) {
            if(csets[i].indexOf(this._accepted_changesets[j]) >= 0) {
                filteredCsets.push(csets[i]);
            }
        }
    }
    return filteredCsets;
};

ChangesetManager.prototype._parseChangesetListBody = function(body){
    var in_a = false,
        csets = [];
    var parser = new htmlparser.Parser({
        onopentag: function(name,attribs) {
                in_a = (name === "a");
        },
        ontext: function(text) { 
            if(in_a) {
                csets.push(text);
            }
        },
        onclosetag: function(tagname) {
            if(in_a && tagname === "a") in_a = false;
        }});
    parser.write(body);
    parser.end();
    return csets;
};

ChangesetManager.prototype._getChangesetListAsync = function(url,callback) {
    var _this = this;
    request(url,function(error,response,body) {
        console.log("REQUEST result came: "+error +" and st: "+response.statusCode+" URL: "+url);
        _this._received++;
        if(!error && response.statusCode == 200) {
            callback(body);
        }
        if(_this._received == _this._awaiting) {
            // This means we have obtained all the hourly changeset
            // lists that we requested originally, and we can proceed
            // to obtain the changesets themselves
            // We should also clean up the state
            // Fire event: hourListsReady
            _this._received = 0;
            _this._awaiting = 0;
            _this.status = CSM_READY;
            _this._recordLastChangeset();
            process.nextTick(function(){_this._afterListDownload();});
        }
    });
};

ChangesetManager.prototype._getHourlyChangesets = function(data, minCsetCount, maxCsetCount) {
    if(minCsetCount === undefined) minCsetCount = 0;
    var fullPath = this._base_url + data.hourPath,
        _this = this,
        body = this._getChangesetListAsync(fullPath, 
                                           function(body) {
                                               var unfiltered_csets = _this._parseChangesetListBody(body);
                                               data.files = _this._filterChangesetList(unfiltered_csets,
                                                                                       minCsetCount,
                                                                                       maxCsetCount);
                                               _this._totalChangesets += data.files.length;
                                           });
};

ChangesetManager.prototype._getCsetOperation = function(filename) {
    for(var i=0; i < this._accepted_changesets.length; i++) {
        if(filename.indexOf(this._accepted_changesets[i]) >= 0) return this._accepted_changesets[i];
    }
    return "unknown";
};

/* Function: _recordLastChangeset
 This function stores the next changeset to start downloading from
*/
ChangesetManager.prototype._recordLastChangeset = function() {
    var lastCs = this._changesetLists[this._changesetLists.length -1];
    lastCs = lastCs.files[lastCs.files.length -1];
    lastCs = parseInt(lastCs);
    this._start_point.setCount(lastCs+1);
    console.log("Setting next new Cset to: " + this._start_point.getPath());
};

/* Function: ChangesetManager.retrieveChangesetList
 Input: from, to - May be Date/hour strings, or ChangesetCounter objects.
                   They express the initial and final
                   date/hour/count of the changelists that we intend to obtain.
 Output: A list of dictionaries of the following shape:
    [{base: "2015/06/30/23/", files: [list of filenames available in 2015/06/30/23]},
     ...,
     ...
     ]
        this list will contain all the available changesets to download within
        from and to, or the maximum limit of changesets to apply.
*/
ChangesetManager.prototype.retrieveChangesetList = function(from,to) {
    if(this.status != CSM_READY) {
        console.log("Not ready to retrieve ChangesetLists");
    }
    this.status = CSM_WAITING_LISTS;

    var fr_cc = (from && from.constructor == ChangesetCounter) ? from : new ChangesetCounter(from),
        t_cc = (to && to.constructor == ChangesetCounter) ? to : new ChangesetCounter(to);
    // We reset the start_point to our new ChangesetCounter
    this._start_point = fr_cc;
    if(!this._changesetLists) {
        this._totalChangesets = 0;
        this._changesetLists = [];
    }
    this._received = 0;
    var count = 0;
    console.log("Starting cycle..."+fr_cc.getPath() +" "+t_cc.getPath());
    while(fr_cc.isSmallerOrEqual(t_cc) && count <= this._HOUR_STEP) {
        var dic = {hourPath: fr_cc.getHourPath(),files: undefined},
            maxCount = (fr_cc.isHourEqual(t_cc) ? t_cc.getCount() : 0);
        this._changesetLists.push(dic);
        this._getHourlyChangesets(dic,fr_cc.getCount(),maxCount);
        count++;

        /* If fr_cc and t_cc have the same hour, then we don't advance fr_cc,
         because we are not yet sure we have all the changesets that will be
         published in that hour (there might be more changesets published 
         afterwards */
        if(!fr_cc.isHourEqual(t_cc)) fr_cc.nextHour();
        else break;
    }
    this._awaiting = count;
};

ChangesetManager.prototype.downloadChangesets = function() {
    if(this.status != CSM_READY) {
        console.log("Not ready to retrieve Changesets");
    }
    this.status = CSM_WAITING_CSETS;
    var csets = [],
        count = 0,
        _this = this;
    for(var i = 0; i < this._changesetLists.length; i++) {
        var files = this._changesetLists[i].files,
            hourPath = this._changesetLists[i].hourPath;
        // If we have run past the threshold, we stop downloading, and get ready to apply
        if(this._CSET_THRESHOLD && count > this._CSET_THRESHOLD) break;
        // If there's something on the csets element, then we don't remove it
        if(this._changesetLists[i].csets !== undefined) continue;

        this._changesetLists[i].csets = [];
        if(!files || !hourPath) continue; // We skip if there are any undefined arguments

        for(var j=0; j< files.length; j++) {
            count += 1;
            var operation = this._getCsetOperation(files[j]),
                url = this._base_url+hourPath+files[j],
                cs = new Changeset({url: url, operation: operation});
            this._changesetLists[i].csets.push(cs);
            cs.downloadAndParse(function(){
                _this._received += 1;
                _this._totalChangesets -= 1;
                if(_this._received == _this._awaiting) {
                    // We have received all the Changesets
                    _this.status = CSM_READY;
                    _this._received = 0;
                    _this._awaiting = 0;
                    process.nextTick(function(){_this._afterChangesetDownload();});
                }
            });
        }
    }
    this._awaiting = count;
};

ChangesetManager.prototype._getChangesetOperations = function(cSet,opStore, ops) {
    var operation = cSet.getOperation(),
        triples = cSet.getTriples(),
        found = {'added': function(triple,store){}, 
                 'removed': function(triple,store) { store.removeTriple(triple);}},
        not_found = {'added': function(triple,store){ store.addTriple(triple);},
                     'removed': function(triple,store){}};
    for(var j = 0; j < triples.length; j++) {
        var tr = triples[j];
        if(opStore.find(tr.subject, tr.predicate, tr.object).length > 0) {
            found[operation](tr,opStore);
        } else {
            not_found[operation](tr,opStore);
            ops.push({operation: operation, triple: tr});
        }
    }
};

/* Function: _computeOperationList
 This function takes the list of changesets, and generates a list of 
 dictionaries of the form {operation: 'add'/'remove', triple: <triple>}.
 The objective of this function is to minimize the amount of added/removed
 triples to the underlying datasource.
*/
ChangesetManager.prototype._computeOperationList = function() {
    var cLists = this._changesetLists,
        store = N3.Store(),
        ops = [];

    for(var i = 0; i < cLists.length; i++) {
        if(cLists[i].csets === undefined) {
            // If there is no changeset downloaded for this,
            // then we should stop, because we've reached the
            // last changeset that has been downloaded
            break;
        }

        var list = cLists[i].csets;
        for(var j = 0; j < list.length; j++) {
            var cSet = list[j];
            this._getChangesetOperations(cSet,store,ops);
        }
    }
    this._opList = ops;
};

ChangesetManager.prototype._applyOperationList = function() {
};

ChangesetManager.prototype._cleanAfterApply = function() {
    var cLists = this._changesetLists;
    for(var i = 0; i < cLists.length; i++) {
        if(cLists[i].csets === undefined) {
            // Slice away the changeset lists that we applied
            this._changesetLists = cLists.slice(i);
            break;
        }
    }
};

ChangesetManager.prototype.applyAndCleanup = function() {
    this._computeOperationList();
    // Now we should add the operations to the dataSource
    this._applyOperationList();
    this._cleanAfterApply();
};

module.exports = ChangesetManager;
