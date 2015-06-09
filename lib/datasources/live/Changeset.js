/** A Changeset object downloads, parses, and stores the contents of a changeset.*/

var zlib = require('zlib'),
    request = require('request'),
    N3 = require('n3');

function Changeset(options) {
    options = options || {};
    this._url = options.url;
    this.operation = options.operation;
    this._fileContents = "";
    this._doneCallback = options.callback || function() {};
    this.done = false;
}


Changeset.prototype._parseFileFillTriples = function() {
    this.triples = [];
    var _this = this,
        parser = N3.Parser();
    parser.parse(_this._fileContents,
                 function(error,triple,prefixes) {
                     if(triple) _this.triples.push(triple);
                     if(triple === null) {
                         _this.done = true;
                         _this._doneCallback();
                     }
                 });
    delete this._fileContents;
};

Changeset.prototype.downloadAndParse = function(callOnEnd) {
    var gunz = zlib.createGunzip(),
        _this = this,
        res = request(this._url).pipe(gunz);
    
    res.on('error', function() {
        console.log("Error downloading changeset - URL: "+_this._url);
        });
    res.on('data',function(chunk){ 
        _this._fileContents += chunk;
    });
    res.on('end', function() {_this._parseFileFillTriples(); 
                              if(callOnEnd) callOnEnd();});
};

module.exports = Changeset;
