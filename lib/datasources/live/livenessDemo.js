var ChangesetManager = require("./ChangesetManager.js"),
    levelup = require('levelup'),
    levelgraph = require('levelgraph'),
    fs = require('fs');

var csm = new ChangesetManager({applyCsetCallback: function(){}}),
    csCount = 0,
    startDate;

var CSM_downloadChangesets = csm.downloadChangesets;
csm.downloadChangesets = function() { console.log("Done downloading changeset lists."); 
                                      console.log("Now we will download a bit over " + csm._CSET_THRESHOLD +" changesets, and apply them.");
                                      startDate = new Date();
                                      (CSM_downloadChangesets.bind(csm))(); };

var CSM_computeOperationList = csm.computeOperationList;
csm.computeOperationList = function() {
    console.log("Now proceeding to calculate a list of operations, and apply...");
    console.log("This is the step that takes longest. The list of added/removed triples is 'optimized'\n"+
                "such that if there's any duplicates, they will be removed.");
    startDate = new Date();
    (CSM_computeOperationList.bind(csm))();
};

var CSM_applyChangesets = csm.applyChangesets;
csm.applyChangesets = function() {
    var endDate = new Date();
    console.log("We have computed "+this._opList.added.length+" additions, and "+
                this._opList.removed.length+" removals applied to the data set.");
    console.log("Took "+((endDate-startDate)/1000)+" seconds.");
    (CSM_applyChangesets.bind(csm))();
};

var CSM_postApplyCleanup = csm.postApplyCleanup;
csm.postApplyCleanup = function() {
    startDate = new Date();
    (CSM_postApplyCleanup.bind(csm))();
};

var CSM_finalizeOrStart = csm.finalizeOrStart;
csm.finalizeOrStart = function() { 
    console.log("Done cleanup.");
    if(!(csm._changesetLists && this._changesetLists.length > 0)) {
        console.log("We're done with the current changeset list. You may run csm.checkForChangesets(); to do a new cycle.");
        return;
    }
    console.log("The cycle will restart now, to finish applying the remaining changesets.");
    (CSM_finalizeOrStart.bind(csm))();
};

console.log("We start with May 5th, 2015, 10am, changeset 10. We download all changesets after it, and apply up\n" +
            "to the latest hour after exceeding 500 changesets.");
csm.retrieveChangesetList("2015/5/5/10/10");
