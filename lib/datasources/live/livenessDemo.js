var ChangesetManager = require("./ChangesetManager.js");
var csm = new ChangesetManager(),
    csCount = 0,
    startDate;
csm._afterListDownload = function() { console.log("Done downloading changeset lists."); 
                                      console.log("The lists contain " +csm._totalChangesets+" changesets.");
                                      console.log("Now we will download a bit over " + csm._CSET_THRESHOLD +" changesets, and apply them.");
                                      csCount = csm._totalChangesets;
                                      startDate = new Date();
                                      csm.downloadChangesets(); };
csm._afterChangesetDownload = function() {
    console.log("We have downloaded a total of " +(csCount - csm._totalChangesets)+" changesets.");
    console.log("Now proceeding to calculate a list of operations, and apply...");
    console.log("This is the step that takes longest. The list of added/removed triples is 'optimized'\n"+
                "such that if there's any duplicates, they will be removed.");
    startDate = new Date();
    csm.computeOperationList();
};

csm._afterComputeOpList = function(ol) {
    var endDate = new Date();
    console.log("We have computed "+ol.length+" operations, and applied to the data set.");
    console.log("Took "+((endDate-startDate)/1000)+" seconds.");
    csm.applyChangesets(ol);
};

csm._afterApply = function() {
    var rmvs = 0,
        adds = 0;
    for(var i = 0; i < csm._opList.length; i ++){
        if(csm._opList[i].operation == 'added') adds++;
        else rmvs++;
    }
    console.log("We have "+adds+" additions, and "+rmvs+" removals. Proceeding now to cleanup.");
    startDate = new Date();
    csm.postApplyCleanup();
};
csm._afterCleanup = function() { 
    console.log("Done cleanup. You may now manually call csm.checkForChangesets(); to let the cycle run again.");
};

console.log("We start with May 5th, 2015, 10am, changeset 10. We download all changesets after it, and apply up\n" +
            "to the latest hour after exceeding 500 changesets.");
csm.retrieveChangesetList("2015/5/5/10/10");
