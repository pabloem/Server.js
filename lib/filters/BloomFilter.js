var Filter = require('bloem').Bloem,
    _ = require('lodash');

function BloomFilter(datasource, query, variable, totalCount, error_p, callback) {
  // estimate k,m. Create bloom
  var m = Math.ceil((-totalCount * Math.log(error_p)) / (Math.LN2 * Math.LN2)),
    k = Math.round((m / totalCount) * Math.LN2),
    bloom = new Filter(m, k);

  var result = datasource.select(_.omit(query, ['limit', 'offset']), callback);

  result.on('data', function (triple) {
    bloom.add(Buffer(triple[variable]));
  });

  result.on('end', function () {
    callback(null, {
      type: 'http://semweb.mmlab.be/ns/amq#BloomFilter',
      variable: variable,
      filter: bloom.bitfield.buffer.toString('base64'),
      m: m,
      k: k
    });
  });
}

module.exports = BloomFilter;
