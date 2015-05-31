var zlib = require('zlib'),
    request = require('request'),
    N3 = require('n3');

function Changeset(options) {
    options = options || {};
    this._url = options.url;
    this._operation = options.operation;
    this._fileContents = "";
}

Changeset.prototype.getOperation = function(){ return this._operation; };
Changeset.prototype.getTriples = function(){ return this._triples; };

Changeset.prototype._parseFileFillTriples = function() {
    this._triples = [];
    var _this = this,
        parser = N3.Parser();
    /* TODO consider parsing serialy, with one only object, rather than
     separately with different parsers 
     TODO - The Parser is ASYNCHRONOUS */
    parser.parse(_this._fileContents,
                 function(error,triple,prefixes) {
                     if(triple) _this._triples.push(triple);
                 });
    delete this._fileContents;
};

Changeset.prototype.downloadAndParse = function(callOnEnd) {
    var gunz = zlib.createGunzip(),
        _this = this,
        res = request(this._url).pipe(gunz);
    
    res.on('error', function() {
        console.log("There was a nasty error!");
        });
    res.on('data',function(chunk){ 
        _this._fileContents += chunk;
    });
    res.on('end', function() {_this._parseFileFillTriples(); 
                              if(callOnEnd) callOnEnd();});
};

module.exports = Changeset;
