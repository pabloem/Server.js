var Datasource = require('../../lib/datasources/Datasource'),
    LiveHdtDatasource = require('../../lib/datasources/LiveHdtDatasource'),
    path = require('path'),
    fs = require('fs');

var exampleHdtFile = path.join(__dirname, '../assets/test.hdt'),
    exampleHdtFileWithBlanks = path.join(__dirname, '../assets/test-blank.hdt'),
    paramDic = { file: exampleHdtFile ,
                 workspace: 'test/assets/workspace/'};

after(function(){
    var databases = ['added.db','removed.db',
                     'added.2','removed.2',
                     'added.3','removed.3',
                     'added.4','removed.4',
                     'added.5','removed.5',
                     'added.6','removed.6',
                     'added.7','removed.7',
                     'added.8','removed.8',
                     'added.9','removed.9'];
    for(var i = 0; i < databases.length ; i++) {
        require('child_process').spawn('rm',['-Rv', path.join(paramDic.workspace,databases[i])]);
    }
});
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
  describe('A LiveHdtDatasource instance with additions', function() {
    paramDic.addedTriplesDb = 'added.5';
    paramDic.removedTriplesDb = 'removed.5';
    paramDic.file = exampleHdtFileWithBlanks;
    var datasource = new LiveHdtDatasource(paramDic);
    before('it should first apply its updates', function(done) {
      var addContent = JSON.parse(asset('../test/assets/triples_file.json')),
      rmvContent = [];
      datasource.applyOperationList({added:addContent, removed:rmvContent},
                                    function() {
                                      datasource._auxiliary.added.get({},function(err,list) {
                                        list.length.should.equal(8);
                                        list[2].subject.should.equal('_:art');
                                      });
                                      done();
                                    });
    });
    after(function (done) { datasource.close(done); });
    itShouldExecute(datasource,
                    'the empty query',
                    { features: { triplePattern: true } },
                    14, 14,
                    [
                      { subject: 'genid:a', predicate: 'b', object: 'c1' },
                      { subject: 'genid:a', predicate: 'b', object: 'c2' },
                      { subject: 'genid:a', predicate: 'b', object: 'c3' },
                      { subject: 'a',       predicate: 'b', object: 'genid:c1' },
                      { subject: 'a',       predicate: 'b', object: 'genid:c2' },
                      { subject: 'a',       predicate: 'b', object: 'genid:c3' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://purl.org/dc/terms/title>',
                        object: '"N-Triples"@en-US' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                        object: '<http://xmlns.com/foaf/0.1/Document>' },
                      { subject: 'genid:art',
                        predicate: '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                        object: '<http://xmlns.com/foaf/0.1/Person>' },
                      { subject: 'genid:dave',
                        predicate: '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                        object: '<http://xmlns.com/foaf/0.1/Person>' },
                      { subject: 'genid:art',
                        predicate: '<http://xmlns.com/foaf/0.1/name>',
                        object: 'Art Barstow' },
                      { subject: 'genid:dave',
                        predicate: '<http://xmlns.com/foaf/0.1/name>',
                        object: 'Dave Beckett' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://xmlns.com/foaf/0.1/maker>',
                        object: 'genid:art' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://xmlns.com/foaf/0.1/maker>',
                        object: 'genid:dave' }
                    ]);
  });
  describe('A LiveHdtDatasource instance with successive updates to auxiliary databases', function() {
    paramDic.addedTriplesDb = 'added.8';
    paramDic.removedTriplesDb = 'removed.8';
    paramDic.file = exampleHdtFileWithBlanks;
    var datasource = new LiveHdtDatasource(paramDic);
    var addContent = JSON.parse(asset('../test/assets/triples_file.json')),
        rmvContent = [];
    before('it should apply its updates first', function(done){
      datasource.applyOperationList(
        {added:addContent, removed:rmvContent},
        function() {
          datasource._auxiliary.added.get({},function(err,list) {
            list.length.should.equal(8);
            list[2].subject.should.equal('_:art');
          });
          addContent = [],
          rmvContent = [{ subject: '_:art',
                          predicate: '<http://xmlns.com/foaf/0.1/name>',
                          object: 'Art Barstow' },
                        { subject: '_:dave',
                          predicate: '<http://xmlns.com/foaf/0.1/name>',
                          object: 'Dave Beckett' }];
          datasource.applyOperationList(
            {added:addContent, removed:rmvContent},
            function() {
              datasource._auxiliary.added.get({},function(err,list) {
                list.length.should.equal(6);
              });
              done();
            });
        });
    });
    itShouldExecute(datasource,
                    'the empty query',
                    { features: { triplePattern: true } },
                    12, 12,
                    [
                      { subject: 'genid:a', predicate: 'b', object: 'c1' },
                      { subject: 'genid:a', predicate: 'b', object: 'c2' },
                      { subject: 'genid:a', predicate: 'b', object: 'c3' },
                      { subject: 'a',       predicate: 'b', object: 'genid:c1' },
                      { subject: 'a',       predicate: 'b', object: 'genid:c2' },
                      { subject: 'a',       predicate: 'b', object: 'genid:c3' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://purl.org/dc/terms/title>',
                        object: '"N-Triples"@en-US' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                        object: '<http://xmlns.com/foaf/0.1/Document>' },
                      { subject: 'genid:art',
                        predicate: '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                        object: '<http://xmlns.com/foaf/0.1/Person>' },
                      { subject: 'genid:dave',
                        predicate: '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                        object: '<http://xmlns.com/foaf/0.1/Person>' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://xmlns.com/foaf/0.1/maker>',
                        object: 'genid:art' },
                      { subject: '<http://www.w3.org/2001/sw/RDFCore/ntriples/>',
                        predicate: '<http://xmlns.com/foaf/0.1/maker>',
                        object: 'genid:dave' }
                    ]);
  });
  describe('A LiveHdtDatasource instance with updates to its HDT database', function() {
    paramDic.addedTriplesDb = 'added.9';
    paramDic.removedTriplesDb = 'removed.9';
    paramDic.file = exampleHdtFile;
    var datasource = new LiveHdtDatasource(paramDic);
    var HDTContents = JSON.parse(asset('../test/assets/testfile.json')),
        addContent = [],
        rmvContent = [],
        notRemoved = [];

    rmvContent = HDTContents.slice();
    rmvContent.splice(110);
    rmvContent.splice(40,60);
    rmvContent.splice(2,18);

    notRemoved = HDTContents.slice();
    notRemoved.splice(100,10);
    notRemoved.splice(20,20);
    notRemoved.splice(0,2);
    before('it will first apply its updates', function(done) {
      datasource.applyOperationList({added:addContent, removed:rmvContent},done);
    });

    var testOffsetCache = function() {
      // Should add function to test integrity of the OffsetCache
    };
    itShouldExecute(datasource,
                    'the empty query with a limit',
                    { limit: 10, features: { triplePattern: true, limit: true } },
                    10, 132,notRemoved.slice(0,10),testOffsetCache);
    itShouldExecute(datasource,
                    'the empty query',
                    { features: { triplePattern: true } },
                    100, 132,notRemoved,testOffsetCache);
    /* IMPORTANT This query tests case 0 */
    itShouldExecute(datasource,
                    'the empty query with an offset',
                    { offset: 10,limit:10, features: { triplePattern: true, offset: true } },
                    10, 132, notRemoved.slice(10,20),testOffsetCache);
    // This query tests case 0 - in fact, the result is the same as the previous
    itShouldExecute(datasource,
                    'a query for an existing predicate',
                    { predicate: 'http://example.org/p1', offset: 10, limit: 10, features: { triplePattern: true, limit: true } },
                    10, 30, notRemoved.slice(10,20));
    itShouldExecute(datasource,
                    'the empty query with a different offset',
                    { offset: 30, features: { triplePattern: true, offset: true } },
                    70, 132,notRemoved.slice(30),testOffsetCache);
    itShouldExecute(datasource,
                    'a query for an existing subject - with offset and limit',
                    { subject: 'http://example.org/s1', offset: 1,limit: 10,features: { triplePattern: true, limit: true } },
                    10, 100,notRemoved.slice(1,11));
                    // NOTE: For this test, we can just sliced the notRemoved list, because the first 11 non-removed
                    //       elements have http://example.org/s1 for subject.
    itShouldExecute(datasource,
                    'a query for an existing predicate',
                    { predicate: 'http://example.org/p1', offset: 2, limit: 10, features: { triplePattern: true, limit: true } },
                    10, 22, notRemoved.slice(2,12));
                    // NOTE: For this test, we can just sliced the notRemoved list, because the first 12 non-removed
                    //       elements have http://example.org/p1 for predicate.
    itShouldExecute(datasource,
                    'a query for an existing object',
                    { object: 'http://example.org/o001',  limit: 10, features: { triplePattern: true, limit: true } },
                    1, 3, // The only non-removed triple with http://example.org/o001 for object.
                    [{"subject": "http://example.org/s3",
                      "predicate": "http://example.org/p2",
                      "object": "http://example.org/o001" }]);
  });
  describe('A LiveHdtDatasource instance with blank nodes', function () {
    paramDic.addedTriplesDb = 'added.6';
    paramDic.removedTriplesDb = 'removed.6';
    paramDic.file = exampleHdtFileWithBlanks;
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
                         expectedResultsCount, expectedTotalCount, expectedTriples,testCallback) {
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
    if(testCallback) {
      it('should run a normal test callback',function() {
        testCallback();
      });
    }
  });
}

function asset(filename) {
  return fs.readFileSync(path.join(__dirname, '../../assets/', filename), 'utf8');
}
