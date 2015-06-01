var Changeset = require('../../../lib/datasources/live/Changeset'),
    fs = require('fs'),
    path = require('path');
/* TODO - add test for download/decompress file */
describe('Changeset', function() {
    describe('A Changeset instance', function() {
        var cs = new Changeset();
        it('should parse an N-Triples formatted file',function() {
            cs._doneCallback = function() { 
                (cs._triples.length).should.equal(8);
                (cs._triples[7].subject).should.equal('_:b0_dave');
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
