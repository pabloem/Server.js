var LiveHdtDatasource = require('../LiveHdtDatasource.js'),
    fs = require('fs');
/*
var add_path = 'workspace/added.db',
    remove_path = 'workspace/removed.db',
    added = levelgraph(levelup(add_path));
//    removed =  levelgraph(levelup(remove_path));

var lgFl ='workspace/demo_logging.log';

function applyChangesets(opList) {
    var ids = [],
        tripleList = opList.removed,
        tripleStore = added;
    console.log("Applying changesets!");
    for(var i = 0; i < tripleList.length; i++) {
        var tr = tripleList[i],
            indx = i;
        fs.appendFileSync(lgFl,"PRE: Triple, i: "+JSON.stringify(tr)+", "+i+"\n");
        tripleStore.get(tr, function(err, list) {
            if(list.length > 0) {
                fs.writeSync(lgFl,"RES: Triple, i: "+JSON.stringify(tr)+", "+i+"\n");
                tripleStore.del(tr);
                ids.push(indx);
            }
            if(indx == tripleList.length-1) { // This is the last triple we look up
                for(var j = ids.length -1; j >= 0; j--) {
                    tripleList.pop(ids[j]);
                }
            }
        });
    }
}
*/
var N3 = require('n3');
var adds = 
"# Comments comments\n\
<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Document> .\n\
<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"N-Triples\"@en-US .\n\
<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:art .\n\
# Arbitrary comments\n\
<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:dave .\n\
_:art <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .\n\
_:art <http://xmlns.com/foaf/0.1/name> \"Art Barstow\".\
_:dave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .\n\
_:dave <http://xmlns.com/foaf/0.1/name> \"Dave Beckett\".\n\
";

var removes = 
"<http://ex.org/data?fragment&page=3> <http://www.w3.org/ns/hydra/core#itemsPerPage> \"100\"^^<http://www.w3.org/2001/XMLSchema#integer>.\n\
<http://ex.org/data?fragment&page=3> <http://www.w3.org/ns/hydra/core#firstPage> <http://ex.org/data?fragment&page=1>.\n\
<http://ex.org/data?fragment&page=3> <http://www.w3.org/ns/hydra/core#previousPage> <http://ex.org/data?fragment&page=2>.\n\
<http://ex.org/data?fragment&page=3> <http://www.w3.org/ns/hydra/core#nextPage> <http://ex.org/data?fragment&page=4>.\n\
<a> <b> <c>.\n\
<a> <d> <e>.\n\
<f> <g> <h>.\n\
";

var addList  = [],
    removeList = [],
    addParser = N3.Parser(),
    removeParser = N3.Parser(),
    opList = {added:addList, removed:removeList};

addParser.parse(adds,function(error,triple,prefixes) { 
    if(error || !triple) return;
    addList.push(triple);
    
});

removeParser.parse(removes,function(error,triple,prefixes) { if(error || !triple) return; removeList.push(triple);});

var options = {file:'blank.hdt',
               workspace:'./workspace/'}; 

var lhds = LiveHdtDatasource(options),
    printer = function() { lhds._auxiliary.added.get({},function(err,list){console.log(list);});};

// Wait a few seconds before applying
lhds.applyOperationList(opList,printer);

// Wait a few seconds before applying
opList = {added:opList.removed, removed:opList.added};
lhds.applyOperationList(opList,printer);
