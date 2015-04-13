var GCSBuilder = require('golombcodedsets').GCSBuilder,
    murmurhash = require('murmurhash'),
    _ = require('lodash'),
    base64 = require('base64-arraybuffer').encode;

function GCSFilter(datasource, query, variable, totalCount, error_p, callback) {
  // estimate k,m. Create bloom
  var gcs = new GCSBuilder(totalCount, 1 / error_p);

  var result = datasource.select(_.omit(query, ['limit', 'offset']), callback);

  result.on('data', function (triple) {
    gcs.add(triple[variable]);
  });

  result.on('end', function () {
    callback(null, {
      type: 'http://semweb.mmlab.be/ns/amq#GCSFilter',
      variable: variable,
      filter: base64(gcs.finalize()),
      p: error_p,
      //hash: {type: 'murmurhash', bits: 32, version: 3}
    });
  });
}

module.exports = GCSFilter;
