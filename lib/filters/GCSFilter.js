var GCSBuilder = require('golombcodedsets').GCSBuilder,
    murmurhash = require('murmurhash'),
    _ = require('lodash'),
    base64 = require('base64-arraybuffer').encode;

function GCSFilter(datasource, query, variable, totalCount, error_p, callback) {
  // Reestimate power of 2
  var pow2 = nearestPow2(1 / error_p);

  // estimate k,m. Create bloom
  var gcs = new GCSBuilder(totalCount, pow2, murmurhash.v3);

  var result = datasource.select(_.omit(query, ['limit', 'offset']), callback);

  result.on('data', function (triple) {
    gcs.add(triple[variable]);
  });

  result.on('end', function () {
    callback(null, {
      type: 'http://semweb.mmlab.be/ns/amq#GCSFilter',
      variable: variable,
      filter: base64(gcs.finalize()),
      p: 1 / pow2
      //hash: {type: 'murmurhash', bits: 32, version: 3}
    });
  });
}

function nearestPow2( aSize ){ return Math.pow( 2, Math.round( Math.log( aSize ) / Math.log( 2 ) ) ); } nearestPow2( 127 ); // 128 nearestPow2( 180 ); // 128 nearestPow2( 200 ); // 256 nearestPow2( 256 ); // 256

module.exports = GCSFilter;
