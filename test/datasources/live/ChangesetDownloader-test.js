var ChangesetDownloader = require('../../../lib/datasources/live/ChangesetDownloader'),
    fs = require('fs'),
    path = require('path');
/* TODO - add test for download/decompress file */
describe('ChangesetDownloader', function() {
    describe('A ChangesetDownloader instance', function() {
        var cd = new ChangesetDownloader();
        it('should parse an N-Triples formatted file',function() {
            var content = asset('triples_file.nt'),
                input = {_fileContents: content};
            cd._doneCallback = function() { 
                (input.triples.length).should.equal(8);
                (input.triples[7].predicate).should.equal('http://xmlns.com/foaf/0.1/name');
                (input.triples[7].object).should.equal('"Dave Beckett"');
                };
            cd._parseFileFillTriples(input);
        });
    });
});

function asset(filename) {
  return fs.readFileSync(path.join(__dirname, '../../assets/', filename), 'utf8');
}
