var zlib = require('zlib'),
    request = require('request');

function Changeset(options) {
    this._url = options.url;
    this._changeset = options.csetNumber;
    this._operation = options.operation;
    this._fileContents = "";
};

Changeset.prototype._parseFileFillTriples = function() {
    var lines = this._fileContents.split("\n");
    this._fileContents = "";
    this._triples = [];
    
    for(var i=0; i<lines.length; i++){
        if(lines[i][0] == "#") continue;
        this._triples.push(lines[i]);
    }
};

Changeset.prototype.downloadAndParse = function() {
    var gunz = zlib.createGunzip(),
        _this = this;
    var res = request(this._url)
            .pipe(gunz);
    
    res.on('data',function(chunk){ 
        _this._fileContents += chunk;
    });
    res.on('end', function() {_this._parseFileFillTriples();});
};

module.exports = Changeset;
