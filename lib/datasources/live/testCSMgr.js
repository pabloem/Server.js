/*
var ChangesetManager = require("./ChangesetManager.js");
var ChangesetCounter = require('./ChangesetCounter.js');
var csm = new ChangesetManager();
csm._start_point = new ChangesetCounter("2015/05/05/10");
csm.checkForChangesets();

var cc = new ChangesetCounter("2015/5/10/21/4");
var cc2 = new ChangesetCounter("2015/5/12/21/4");
console.log(cc.getPath());

for(var i=0; i < 30; i++) {
    cc.nextHour();
    console.log(cc.getPath());
    console.log(cc.getHourPath());
}
var cc2 = new ChangesetCounter([2015,5,30,21,4]);
console.log(cc2.getPath());
console.log(cc.isSmallerOrEqual(cc2));
console.log(cc2.isSmallerOrEqual(cc));
*/

//console.log(csm._getSynchronously(csm.getFullHourPath(cc)));
//console.log(csm._getHourlyChangesets(cc));
/*
csm.retrieveChangesetList("2015/5/5/10/10","2015/5/5/20");
csm.downloadChangesets();
*/
/*console.log(allCsets.length);
for(var i=0; i<allCsets.length; i++){
    console.log(allCsets[i].base + " " +allCsets[i].files.length);
}


var Changeset = require("./Changeset.js");
var cs = new Changeset({url:"http://live.dbpedia.org/changesets/2015/05/17/17/000002.removed.nt.gz",
                        csetNumber: 2,
                        operation: "removed"});
cs.downloadAndParse();


*/
