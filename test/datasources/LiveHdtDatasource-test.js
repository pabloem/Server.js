var Datasource = require('../../lib/datasources/Datasource'),
    LiveHdtDatasource = require('../../lib/datasources/LiveHdtDatasource'),
    path = require('path'),
    fs = require('fs');

var exampleHdtFile = path.join(__dirname, '../assets/test.hdt'),
    exampleHdtFileWithBlanks = path.join(__dirname, '../assets/test-blank.hdt'),
    blankFile = path.join(__dirname, '../assets/blank-file.hdt'),
    paramDic = { file: exampleHdtFile ,
                 workspace: 'test/assets/workspace/'};

describe('LiveHdtDatasource', function () {
  describe('The LiveHdtDatasource module', function () {
    it('should be a function', function () {
      LiveHdtDatasource.should.be.a('function');
    });
    it('should create LiveHdtDatasource objects', function (done) {
      paramDic.addedTriplesDb = 'added.2';
      paramDic.removedTriplesDb = 'removed.2';
      var instance = LiveHdtDatasource(paramDic);
      instance.should.be.an.instanceof(LiveHdtDatasource);
      instance.close(done);
    });

    it('should create Datasource objects', function (done) {
      paramDic.addedTriplesDb = 'added.3';
      paramDic.removedTriplesDb = 'removed.3';
      var instance = new LiveHdtDatasource(paramDic);
      instance.should.be.an.instanceof(Datasource);
      instance.close(done);
    });
  });

  describe('A LiveHdtDatasource instance for an example HDT file', function () {
    paramDic.addedTriplesDb = 'added.4';
    paramDic.removedTriplesDb = 'removed.4';
    var datasource = new LiveHdtDatasource(paramDic);
    after(function (done) { datasource.close(done); });

    itShouldExecute(datasource,
      'the empty query',
      { features: { triplePattern: true } },
      132, 132);

    itShouldExecute(datasource,
      'the empty query with a limit',
      { limit: 10, features: { triplePattern: true, limit: true } },
      10, 132);

    itShouldExecute(datasource,
      'the empty query with an offset',
      { offset: 10, features: { triplePattern: true, offset: true } },
      122, 132);

    itShouldExecute(datasource,
      'a query for an existing subject',
      { subject: 'http://example.org/s1',   limit: 10, features: { triplePattern: true, limit: true } },
      10, 100);

    itShouldExecute(datasource,
      'a query for a non-existing subject',
      { subject: 'http://example.org/p1',   limit: 10, features: { triplePattern: true, limit: true } },
      0, 0);

    itShouldExecute(datasource,
      'a query for an existing predicate',
      { predicate: 'http://example.org/p1', limit: 10, features: { triplePattern: true, limit: true } },
      10, 20);

    itShouldExecute(datasource,
      'a query for a non-existing predicate',
      { predicate: 'http://example.org/s1', limit: 10, features: { triplePattern: true, limit: true } },
      0, 0);

    itShouldExecute(datasource,
      'a query for an existing object',
      { object: 'http://example.org/o001',  limit: 10, features: { triplePattern: true, limit: true } },
      3, 3);

    itShouldExecute(datasource,
      'a query for a non-existing object',
      { object: 'http://example.org/s1',    limit: 10, features: { triplePattern: true, limit: true } },
      0, 0);
  });
  describe('A LiveHdtDatasource instance with updates', function() {
    paramDic.addedTriplesDb = 'added.5';
    paramDic.removedTriplesDb = 'removed.5';
    paramDic.file = blankFile;
    var datasource = new LiveHdtDatasource(paramDic);
    after(function (done) { datasource.close(done); });
    var addContent = JSON.parse(asset('../test/assets/triples_file.json')),
        rmvContent = [];
    datasource.applyOperationList({added:addContent, removed:rmvContent},
                                 function() {
                                   datasource._auxiliary.added.get({},function(err,list) {
                                       list.length.should.equal(8);
                                       console.log(list);
                                   });
                                 });
  });
  describe('A LiveHdtDatasource instance with blank nodes', function () {
    paramDic.addedTriplesDb = 'added.6';
    paramDic.removedTriplesDb = 'removed.6';
    var datasource = new LiveHdtDatasource(paramDic);
    after(function (done) { datasource.close(done); });

    itShouldExecute(datasource,
      'the empty query',
      { features: { triplePattern: true } },
      6, 6,
      [
        { subject: 'genid:a', predicate: 'b', object: 'c1' },
        { subject: 'genid:a', predicate: 'b', object: 'c2' },
        { subject: 'genid:a', predicate: 'b', object: 'c3' },
        { subject: 'a',       predicate: 'b', object: 'genid:c1' },
        { subject: 'a',       predicate: 'b', object: 'genid:c2' },
        { subject: 'a',       predicate: 'b', object: 'genid:c3' },
      ]);

    itShouldExecute(datasource,
      'a query for a blank subject',
      { suject: '_:a', features: { triplePattern: true } },
      6, 6);

    itShouldExecute(datasource,
      'a query for a IRI that corresponds to a blank node as subject',
      { subject: 'genid:a', features: { triplePattern: true } },
      3, 3,
      [
        { subject: 'genid:a', predicate: 'b', object: 'c1' },
        { subject: 'genid:a', predicate: 'b', object: 'c2' },
        { subject: 'genid:a', predicate: 'b', object: 'c3' },
      ]);

    itShouldExecute(datasource,
      'a query for a IRI that corresponds to a blank node as object',
      { object: 'genid:c1', features: { triplePattern: true } },
      1, 1,
      [
        { subject: 'a', predicate: 'b', object: 'genid:c1' },
      ]);
  });

  describe('A LiveHdtDatasource instance with blank nodes and a blank node prefix', function () {
    paramDic.addedTriplesDb = 'added.7';
    paramDic.removedTriplesDb = 'removed.7';
    paramDic.blankNodePrefix = 'http://example.org/.well-known/genid/';
    var datasource = new LiveHdtDatasource(paramDic);
    after(function (done) { datasource.close(done); });

    itShouldExecute(datasource,
      'the empty query',
      { features: { triplePattern: true } },
      6, 6,
      [
        { subject: 'http://example.org/.well-known/genid/a', predicate: 'b', object: 'c1' },
        { subject: 'http://example.org/.well-known/genid/a', predicate: 'b', object: 'c2' },
        { subject: 'http://example.org/.well-known/genid/a', predicate: 'b', object: 'c3' },
        { subject: 'a', predicate: 'b', object: 'http://example.org/.well-known/genid/c1' },
        { subject: 'a', predicate: 'b', object: 'http://example.org/.well-known/genid/c2' },
        { subject: 'a', predicate: 'b', object: 'http://example.org/.well-known/genid/c3' },
      ]);

    itShouldExecute(datasource,
      'a query for a blank subject',
      { suject: '_:a', features: { triplePattern: true } },
      6, 6);

    itShouldExecute(datasource,
      'a query for a IRI that corresponds to a blank node as subject',
      { subject: 'http://example.org/.well-known/genid/a', features: { triplePattern: true } },
      3, 3,
      [
        { subject: 'http://example.org/.well-known/genid/a', predicate: 'b', object: 'c1' },
        { subject: 'http://example.org/.well-known/genid/a', predicate: 'b', object: 'c2' },
        { subject: 'http://example.org/.well-known/genid/a', predicate: 'b', object: 'c3' },
      ]);

    itShouldExecute(datasource,
      'a query for a IRI that corresponds to a blank node as object',
      { object: 'http://example.org/.well-known/genid/c1', features: { triplePattern: true } },
      1, 1,
      [
        { subject: 'a', predicate: 'b', object: 'http://example.org/.well-known/genid/c1' },
      ]);
  });
});

function itShouldExecute(datasource, name, query,
                         expectedResultsCount, expectedTotalCount, expectedTriples) {
  describe('executing ' + name, function () {
    var resultsCount = 0, totalCount, triples = [];
    before(function (done) {
      var result = datasource.select(query);
      result.on('metadata', function (metadata) { totalCount = metadata.totalCount; });
      result.on('data', function (triple) { resultsCount++; expectedTriples && triples.push(triple); });
      result.on('end', done);
    });

    it('should return the expected number of triples', function () {
      expect(resultsCount).to.equal(expectedResultsCount);
    });

    it('should emit the expected total number of triples', function () {
      expect(totalCount).to.equal(expectedTotalCount);
    });

    if (expectedTriples) {
      it('should emit the expected triples', function () {
        expect(triples.length).to.equal(expectedTriples.length);
        for (var i = 0; i < expectedTriples.length; i++)
          triples[i].should.deep.equal(expectedTriples[i]);
      });
    }
  });
}

function asset(filename) {
  return fs.readFileSync(path.join(__dirname, '../../assets/', filename), 'utf8');
}
