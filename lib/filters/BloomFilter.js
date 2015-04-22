var Filter = require('bloem').Bloem,
  _ = require('lodash'),
  murmurhash = require('murmurhash'),
  fs = require('fs'),
  path = require('path');

function BloomFilter(datasource, query, variable, totalCount, error_p, callback) {
  var fileName = path.join('./filter/bloom', '' + murmurhash.v3(query.subject ? 'subject=' + query.subject : '' +
    query.predicate ? 'predicate=' + query.predicate : '' +
    query.object ? 'object=' + query.object : '') + '.json');
  fs.readFile(fileName, 'utf8', function (err, data) {
    if (err) {
      // estimate k,m. Create bloom
      var m = Math.ceil((-totalCount * Math.log(error_p)) / (Math.LN2 * Math.LN2)),
        k = Math.round((m / totalCount) * Math.LN2),
        bloom = new Filter(m, k);

      var result = datasource.select(_.omit(query, ['limit', 'offset']), callback);

      result.on('data', function (triple) {
        bloom.add(Buffer(triple[variable]));
      });

      result.on('end', function () {
        var result = {
          type: 'http://semweb.mmlab.be/ns/amq#BloomFilter',
          variable: variable,
          filter: bloom.bitfield.buffer.toString('base64'),
          m: m,
          k: k
        };
        fs.writeFile(fileName, JSON.stringify(result), function (err) {
          if (err)
            console.error('Bloom not saved %s', err);
          else
            console.error('Bloom saved %s', fileName);
        });
        callback(null, result);
      });
    } else {
      console.error('Local Bloom found %s', fileName);
      callback(null, JSON.parse(data));
    }
  });
}

module.exports = BloomFilter;
