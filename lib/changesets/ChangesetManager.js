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
    this._file_extension = options.file_extension || '.nt.gz';
    this._accepted_changesets = options.accepted_changesets || 
        ['added','removed'];

    this._MAX_CSETS = options.max_csets || 500;
    this._HOUR_STEP = options.hour_step || 10;
    this._start_point = undefined;
    this._status = CSM_READY;

    // We set the changeset counter to point to the newest changeset that we 
    // should attempt to download
};

/* This function is super ugly. Apologies. */
ChangesetManager.prototype._filterChangesetList = function(csets) {
    var filteredCsets = [];
    for(var i = 0; i < csets.length; i ++) {
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
            _this._status = CSM_READY;
        }
    });
};

ChangesetManager.prototype._getHourlyChangesets = function(data) {
    var fullPath = this._base_url + data.hourPath,
        _this = this;
    var body = this._getChangesetListAsync(fullPath,
                                           function(body) {
                                               var unfiltered_csets = _this._parseChangesetListBody(body);
                                               data.files = _this._filterChangesetList(unfiltered_csets);
                                               _this._totalChangesets += data.files.length;
                                           });
};


/* Function: ChangesetManager.retrieveChangesetList
 Input: from, to - Date/hour strings, containing the initial and final
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
    if(this._status != CSM_READY) {
        console.log("Not ready to retrieve ChangesetLists");
    }
    this._status = CSM_WAITING_LISTS;

    var fr_cc = new ChangesetCounter(from),
        t_cc = new ChangesetCounter(to);
    
    if(!this._changesetLists) {
        this._totalChangesets = 0;
        this._changesetLists = [];
    }
    this._received = 0;

    var count = 0;
    while(fr_cc.isSmallerOrEqual(t_cc) && count < this._HOUR_STEP) {
        var dic = {hourPath: fr_cc.getHourPath(),files: undefined};
        this._changesetLists.push(dic);
        this._getHourlyChangesets(dic);
        count++;

        fr_cc.nextHour();
    }
    this._awaiting = count;
};

ChangesetManager.prototype._getCsetOperation = function(filename) {
    for(var i=0; i < this._accepted_changesets.length; i++) {
        if(filename.indexOf(this._accepted_changesets[i]) >= 0) return this._accepted_changesets[i];
    }
    return "unknown";
};

ChangesetManager.prototype.downloadChangesetsInLists = function() {
    if(this._status != CSM_READY) {
        console.log("Not ready to retrieve Changesets");
    }
    this._status = CSM_WAITING_CSETS;
    var csets = [],
        count = 0,
        _this = this;
    for(var i = 0; i < this._changesetLists.length; i++) {
        var files = this._changesetLists[i].files,
            hourPath = this._changesetLists[i].hourPath;
        if(!files || !hourPath) continue; // We skip if there are any undefined arguments

        for(var j=0; j< files.length; j++) {
            count += 1;
            var operation = this._getCsetOperation(files[j]),
                csetNumber = parseInt(files[j]),
                url = this._base_url+hourPath+files[j];
            var cs = new Changeset({url: url, operation: operation, csetNumber: csetNumber});
            csets.push(cs);
            cs.downloadAndParse(function(){
                _this._received += 1;
                if(_this._received == _this._awaiting) {
                    // We have received all the Changesets
                    _this._status = CSM_READY;
                    _this._received = 0;
                    _this._awaiting = 0;
                }
            });
        }
    }
    this._awaiting = count;
};

module.exports = ChangesetManager;
