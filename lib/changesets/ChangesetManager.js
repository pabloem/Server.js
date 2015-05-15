var request = require('request'),
    ChangesetCounter = require('./ChangesetCounter.js');

function ChangesetManager(options) {
    options = options || {};
    this._base_url = options.baseUrl || 'http://live.dbpedia.org/changesets/';
    this._file_extension = options.file_extension || '.nt.gz';
    this._last_changeset = options.last_changeset;

    // We set the changeset counter to point to the newest changeset that we 
    // should attempt to download
};

ChangesetManager.prototype._generate_url = function() {
    var add = this._cc.getPath();
    return this._base_url+add;
};

ChangesetManager.prototype.retrieveChangesets = function(from,to) {
    var fr = from.slice(),
        t = to.slice();
    while(fr.length < 5){ fr.push(0); }
    while(t.length < 5){ t.push(0); }

    var fr_cc = new ChangesetCounter(fr);
    var t_cc = new ChangesetCounter(t);
};

module.exports = ChangesetManager;
