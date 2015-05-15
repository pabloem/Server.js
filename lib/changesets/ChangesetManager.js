var http = require('http');

function ChangesetManager(options) {
    options = options || {};
    this._base_url = options.baseUrl || 'live.dbpedia.com/changesets/';
};

ChangesetManager.prototype._generate_url = function() {
    return this._base_url;
};

ChangesetManager.prototype.retrieveChangesets = function(from,to) {
    var _this = this;
    var req = http.request(this._get_url(), function(req) {});
};
