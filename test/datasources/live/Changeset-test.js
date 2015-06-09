var Changeset = require('../../../lib/datasources/live/Changeset'),
    fs = require('fs'),
    path = require('path');
/* TODO - add test for download/decompress file */
describe('Changeset', function() {
    describe('A Changeset instance', function() {
        var cs = new Changeset();
        it('should parse an N-Triples formatted file',function() {
            cs._doneCallback = function() { 
                (cs.triples.length).should.equal(8);
                (cs.triples[7].predicate).should.equal('http://xmlns.com/foaf/0.1/name');
                (cs.triples[7].object).should.equal('"Dave Beckett"');
                };
            var content = asset('triples_file.nt');
            cs._fileContents = content;
            cs._parseFileFillTriples();
        });
    });
});

function asset(filename) {
  return fs.readFileSync(path.join(__dirname, '../../assets/', filename), 'utf8');
}
