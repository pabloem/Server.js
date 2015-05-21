var request = require('request'),
    ChangesetCounter = require('./ChangesetCounter.js'),
    httpSync = require('http-sync'),
    htmlparser = require('htmlparser2');

function ChangesetManager(options) {
    options = options || {};
    this._base_url = options.baseUrl || 'http://live.dbpedia.org/changesets/';
    this._file_extension = options.file_extension || '.nt.gz';
    this._accepted_changesets = options.accepted_changesets || 
        ['added','removed'];
    this._last_changeset = options.last_changeset;

    // We set the changeset counter to point to the newest changeset that we 
    // should attempt to download
};

ChangesetManager.prototype.getFullHourPath = function(cCounter) {
    return this._base_url + cCounter.getHourPath();
};

/* Our requests are run synchronously, since the polling
 is already done asynchronously */
ChangesetManager.prototype._getSynchronously = function(url) {
    //console.log("URL is: "+url);
    var request = httpSync.request({
        method: 'GET', url: url});

    var timedout = false;
    /*request.setTimeout(1000, function() {
        console.log("Request Timed out!");
        timedout = true;
    });*/
    var response = request.end();
    return timedout ? 
        "ERROR" :
        response.body+""; // Converting the body (buffer) to string
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

/* This function is super ugly. Apologies. */
ChangesetManager.prototype._filterChangesets = function(csets) {
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

ChangesetManager.prototype._getHourlyChangesets = function(cCounter) {
    var fullPath = this.getFullHourPath(cCounter);
    var body = this._getSynchronously(fullPath);

    // Variables for parsing
    var unfiltered_csets = this._parseChangesetListBody(body);
    return this._filterChangesets(unfiltered_csets);
};

ChangesetManager.prototype.retrieveChangesets = function(from,to) {
    var fr_cc = new ChangesetCounter(from);
    var t_cc = new ChangesetCounter(to);
    
    var allChangesets = [];
    while(fr_cc.isSmallerOrEqual(t_cc)) {
        var changesets = this._getHourlyChangesets(fr_cc);
        allChangesets.push({base: fr_cc.getHourPath(),
                            files: changesets});
        fr_cc.nextHour();
    }
    return allChangesets;
    // At this point, we have downloaded all the changeset names for
    // the changesets in our range (more, gotta solve that)
//    for(var i =0; i < allChangesets.length; i++) {
        
//    }
};

module.exports = ChangesetManager;
