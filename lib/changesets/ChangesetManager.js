var request = require('request'),
    ChangesetCounter = require('./ChangesetCounter.js'),
    htmlparser = require('htmlparser2'),
    Changeset = require('./Changeset.js');

var CSM_WAITING_LISTS = "WAITING CSET LISTS",
    CSM_WAITING_CSETS = "WAITING CHANGESETS",
    CSM_READY = "READY";

function ChangesetManager(options) {
    options = options || {};
    this._base_url = options.baseUrl || 'http://live.dbpedia.org/changesets/';
    this._accepted_changesets = options.accepted_changesets || 
        ['added','removed'];

    this._start_point = new ChangesetCounter(options.lastCset);
    this.status = CSM_READY;
    this._HOUR_STEP = options.hour_step || 20;
    this._CSET_THRESHOLD = options.cset_threshold || 0;

    // We set the changeset counter to point to the newest changeset that we 
    // should attempt to download
}

ChangesetManager.prototype.checkForChangesets = function() {
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
            process.nextTick(function(){_this.downloadChangesets();});
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
    this._recordLastChangeset();
    this.status = CSM_WAITING_CSETS;
    var csets = [],
        count = 0,
        _this = this;
    for(var i = 0; i < this._changesetLists.length; i++) {
        var files = this._changesetLists[i].files,
            hourPath = this._changesetLists[i].hourPath;
        if(this._changesetLists[i].csets === undefined) continue;
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
                if(_this._received == _this._awaiting) {
                    // We have received all the Changesets
                    _this.status = CSM_READY;
                    _this._received = 0;
                    _this._awaiting = 0;
                    if(_this._CSET_THRESHOLD && _this._CSET_THRESHOLD < _this._totalChangesets) {
                        process.nextTick(function(){_this.applyAndCleanup();});
                    }
                }
            });
        }
    }
    this._awaiting = count;
};

ChangesetManager.prototype.applyAndCleanup = function() {
    console.log("Removing changeset lists");
    delete this._changesetLists;
    this._totalChangesets = 0;
};

module.exports = ChangesetManager;
