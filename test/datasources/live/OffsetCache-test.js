var OffsetCache = require('../../../lib/datasources/live/OffsetCache');
describe('OffsetCache', function() {
  describe('An OffsetCache instance', function() {
    var options = {query_limit:3,
                   offset_limit: 5},
        oc = new OffsetCache(options),
        query = {subject:'John', predicate:'Loves', object:'Roses'};

    it('returns 0 if there is no known offset', function() {
      var ofst = oc.getClosestLowerOffset(query,350);
      ofst.virtual.should.equal(0);
      ofst.real.should.equal(0);
    });
    it('returns lower offsets only', function() {
      oc.addToCache(query,400,403);
      oc.addToCache(query,300,301);
      var ofst = oc.getClosestLowerOffset(query,350);
      ofst.virtual.should.equal(300);
      ofst.real.should.equal(301);
    });
    it('does not go beyond its query limit',function() {
      var query1 = {subject:'Lawrence',predicate:'Hates',object:'Roses'},
          query2 = {subject:'Rose',predicate:'Eats',object:'Spaghetti'},
          query3 = {subject:'Brandon',predicate:'Eats',object:'Roses'};
      oc._qrList.length.should.equal(1);
      oc.addToCache(query1,10,10);
      oc._qrList.length.should.equal(2);
      oc.addToCache(query2,10,11);
      oc._qrList.length.should.equal(3);
      oc.addToCache(query3,10,15);
      oc._qrList.length.should.equal(3);
    });
    it('does not go beyond its offset limit',function() {
      var query4 = {subject:'Azema',predicate:'speaks',object:'Russian'},
          id = oc.getId(query4);
      oc.addToCache(query4,0,0);
      oc._queryCache[id].virtual.length.should.equal(1);
      oc.addToCache(query4,10,20);
      oc._queryCache[id].virtual.length.should.equal(2);
      oc.addToCache(query4,20,33);
      oc._queryCache[id].virtual.length.should.equal(3);
      oc.addToCache(query4,50,70);
      oc._queryCache[id].virtual.length.should.equal(4);
      oc.addToCache(query4,90,190);
      oc._queryCache[id].virtual.length.should.equal(5);
      oc.addToCache(query4,300,400);
      oc._queryCache[id].virtual.length.should.equal(5);
      oc._queryCache[id].real.length.should.equal(5);
    });
    it('returns to empty state if flushed',function(){
      var query4 = {subject:'Azema',predicate:'speaks',object:'Russian'};
      var res = oc.getClosestLowerOffset(query4,500);
      res.virtual.should.equal(300);
      res.real.should.equal(400);
      oc.flushCache();
      res = oc.getClosestLowerOffset(query4,500);
      res.virtual.should.equal(0);
      res.real.should.equal(0);
    });
  });
});
         
