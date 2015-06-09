/** A ChangesetDownloader downloads, parses, and stores the contents of a changeset.*/

var zlib = require('zlib'),
    request = require('request'),
    N3 = require('n3');

function ChangesetDownloader(options) {
}

ChangesetDownloader.prototype._parseFileFillTriples = function(input) {
    input.triples = [];
    var parser = N3.Parser(),
        _this = this;
    parser.parse(input._fileContents,
                 function(error,triple,prefixes) {
                     if(triple) input.triples.push(triple);
                     if(triple === null && _this._doneCallback) {
                         _this._doneCallback();
                     }
                 });
    delete input._fileContents;
};

ChangesetDownloader.prototype.downloadAndParse = function(input,callback) {
    var gunz = zlib.createGunzip(),
        _this = this,
        res = request(input.url).pipe(gunz);

    input._fileContents = "";
    
    res.on('error', function() {
        console.log("Error downloading changeset - URL: "+input.url);
        });
    res.on('data',function(chunk){ 
        input._fileContents += chunk;
    });
    res.on('end', function() {_this._parseFileFillTriples(input); 
                              if(callback) callback();});
};

module.exports = ChangesetDownloader;
