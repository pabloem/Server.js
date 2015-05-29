var Changeset = require('../../lib/changesets/Changeset'),
    fs = require('fs'),
    path = require('path');
describe('Changeset', function() {
    describe('A Changeset instance', function() {
        var cs = new Changeset();
        it('should parse an N-Triples formatted file',function() {
            var content = asset('triples_file.nt');
            cs._fileContents = content;
            cs._parseFileFillTriples();
            // TODO - The parser is asynchronous, so gotta find a way
            // To do a synchronous check.
            // Also relevant in Changeset.js
        });
    });
});

function asset(filename) {
  return fs.readFileSync(path.join(__dirname, '../assets/', filename), 'utf8');
}
