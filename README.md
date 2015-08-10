# Linked Data Fragments Server <img src="http://linkeddatafragments.org/images/logo.svg" width="100" align="right" alt="" />
On today's Web, Linked Data is published in different ways,
which include [data dumps](http://downloads.dbpedia.org/3.9/en/),
[subject pages](http://dbpedia.org/page/Linked_data),
and [results of SPARQL queries](http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=CONSTRUCT+%7B+%3Fp+a+dbpedia-owl%3AArtist+%7D%0D%0AWHERE+%7B+%3Fp+a+dbpedia-owl%3AArtist+%7D&format=text%2Fturtle).
We call each such part a [**Linked Data Fragment**](http://linkeddatafragments.org/).

The issue with the current Linked Data Fragments
is that they are either so powerful that their servers suffer from low availability rates
([as is the case with SPARQL](http://sw.deri.org/~aidanh/docs/epmonitorISWC.pdf)),
or either don't allow efficient querying.

Instead, this server offers **[Triple Pattern Fragments](http://www.hydra-cg.com/spec/latest/triple-pattern-fragments/)**.
Each Triple Pattern Fragment offers:

- **data** that corresponds to a _triple pattern_
  _([example](http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3ARestaurant))_.
- **metadata** that consists of the (approximate) total triple count
  _([example](http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=))_.
- **controls** that lead to all other fragments of the same dataset
  _([example](http://data.linkeddatafragments.org/dbpedia?subject=&predicate=&object=%22John%22%40en))_.

An example server is available at [data.linkeddatafragments.org](http://data.linkeddatafragments.org/).


## Install the server

This server requires [Node.js](http://nodejs.org/) 0.10 or higher
and is tested on OSX and Linux.
To install, execute:
```bash
$ [sudo] npm install -g ldf-server
```


## Use the server

### Configure the data sources

First, create a configuration file `config.json` similar to `config-example.json`,
in which you detail your data sources.
For example, this configuration uses an [HDT file](http://www.rdfhdt.org/)
and a [SPARQL endpoint](http://www.w3.org/TR/sparql11-protocol/) as sources:
```json
{
  "title": "My Linked Data Fragments server",
  "datasources": {
    "dbpedia": {
      "title": "DBpedia 2014",
      "type": "HdtDatasource",
      "description": "DBpedia 2014 with an HDT back-end",
      "settings": { "file": "data/dbpedia2014.hdt" }
    },
    "dbpedia-sparql": {
      "title": "DBpedia 3.9 (Virtuoso)",
      "type": "SparqlDatasource",
      "description": "DBpedia 3.9 with a Virtuoso back-end",
      "settings": { "endpoint": "http://dbpedia.restdesc.org/", "defaultGraph": "http://dbpedia.org" }
    }
  }
}
```

The following sources are supported out of the box:
- HDT files ([`HdtDatasource`](https://github.com/LinkedDataFragments/Server.js/blob/master/lib/datasources/HdtDatasource.js) with `file` setting)
- N-Triples documents ([`TurtleDatasource`](https://github.com/LinkedDataFragments/Server.js/blob/master/lib/datasources/TurtleDatasource.js) with `url` setting)
- Turtle documents ([`TurtleDatasource`](https://github.com/LinkedDataFragments/Server.js/blob/master/lib/datasources/TurtleDatasource.js) with `url` setting)
- JSON-LD documents ([`JsonLdDatasource`](https://github.com/LinkedDataFragments/Server.js/blob/master/lib/datasources/JsonLdDatasource.js) with `url` setting)
- SPARQL endpoints ([`SparqlDatasource`](https://github.com/LinkedDataFragments/Server.js/blob/master/lib/datasources/SparqlDatasource.js) with `endpoint` and optionally `defaultGraph` settings)

Support for new sources is possible by implementing the [`Datasource`](https://github.com/LinkedDataFragments/Server.js/blob/master/lib/datasources/Datasource.js) interface.

### Start the server

After creating a configuration file, execute
```bash
$ ldf-server config.json 5000 4
```
Here, `5000` is the HTTP port on which the server will listen,
and `4` the number of worker processes.

Now visit `http://localhost:5000/` in your browser.

### Reload running server

You can reload the server without any downtime
in order to load a new configuration or version.
<br>
In order to do this, you need the process ID of the server master process.
<br>
One possibility to obtain this are the server logs:
```bash
$ bin/ldf-server config.json
Master 28106 running.
Worker 28107 running on http://localhost:3000/.
```

If you send the server a `SIGHUP` signal:
```bash
$ kill -s SIGHUP 28106
```
it will reload by replacing its workers.

Note that crashed or killed workers are always replaced automatically.

### Using the `LiveHdtDatasource`
The `LiveHdtDatasource` was developed by [Pablo Estrada](http://github.com/pabloem/) as part of a project for
the Google Summer of Code of 2015. This datasource allows the Linked Data Fragments Server to keep an updated
version of the DBPedia, while still being scalable.

The `LiveHdtDatasource` used an HDT file as data source. It periodically polls the [DBPedia Live Feed](http://live.dbpedia.org/changesets/)
for updates to the DBPedia dataset. When there are new updates, it downloads them and inserts them in
temporary databases; which are used along with the base HDT file. Then, it periodically generates a new HDT
file, that incorporates the changesetss it had downloaded previously.

The `LiveHdtDatasource` requires a 'workspace' directory for its temporary databases, and the newly generated
HDT files. It is recommended that after first initialization, the `LiveHdtDatasource` is allowed to
manage its workspace independently.

A LiveHdtDatasource requires the following settings to be configured to run properly:
* `pollingInterval` - This is the period, in minutes, between each polling for new changesets in the DBPecia Live Feed.
* `regeneratingInterval` - This is the period, in minutes, between each time the HDT is regenerated.
* `file` - This is the HDT file containing the data. The `LiveHdtDatasource` will ignore, and delete the file once a new HDT file has been generated.
* `workspace` - This is the workspace directory where the `LiveHdtDatasource` manages its data. The directory must exist.
* `latestChangeset` - This is the latest changeset that has been added to the HDT file, written as 'Year/Month/Day/Number'. Once the `LiveHdtDatasource` has started
polling and applying changesets on it's own, it will ignore this parameter. Nonetheless, it's very important to add this information on first startup.
* `addedTriplesDb`/`removedTriplesDb` - These are the added/removed triples databases. It defaults to 'added.db', and 'removed.db' within the workspace. - It is recommended to leave this property unset.
* `regeneratorScript` - This is the script that regenerates the HDT file. It defaults to `./consolidate.sh` in the `bin/` directory.
* `changesetThreshold` - This is the maximum number of changesets to be applied on a single apply cycle. If more than this number of
changesets is retrieved, they will be applied in several cycles. Default is 500.
* `hourStep` - This is the maximum number of hours to try to download at once. This generally does not matter, since the `pollingInterval`
is smaller, but when we need to update the HDT file, this limit will play a role. Default is 25.

The default regenerator script uses the [hdt-iris](http://github.com/pabloem/hdt-iris), so it is recommended to
install it, and add its appropriate location in the `bin/consolidate.sh` file.

### _(Optional)_ Set up a reverse proxy

A typical Linked Data Fragments server will be exposed
on a public domain or subdomain along with other applications.
Therefore, you need to configure the server to run behind an HTTP reverse proxy.
<br>
To set this up, configure the server's public URL in your server's `config.json`:
```json
{
  "title": "My Linked Data Fragments server",
  "baseURL": "http://data.example.org/",
  "datasources": { … }
}
```
Then configure your reverse proxy to pass requests to your server.
Here's an example for [nginx](http://nginx.org/):
```nginx
server {
  server_name data.example.org;

  location / {
    proxy_pass http://127.0.0.1:3000$request_uri;
    proxy_set_header Host $http_host;
    proxy_pass_header Server;
  }
}
```
Change the value `3000` into the port on which your Linked Data Fragments server runs.

If you would like to proxy the data in a subfolder such as `http://example.org/my/data`,
modify the `baseURL` in your `config.json` to `"http://example.org/my/data"`
and change `location` from `/` to `/my/data` (excluding a trailing slash).

## License
The Linked Data Fragments server is written by [Ruben Verborgh](http://ruben.verborgh.org/).

This code is copyrighted by [iMinds – Ghent University](http://mmlab.be/)
and released under the [MIT license](http://opensource.org/licenses/MIT).
