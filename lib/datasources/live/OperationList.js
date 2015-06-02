var N3 = require('n3');
var OperationList = function(options){
    options = options || {};
    this.opStore = N3.Store();
    this.opList = [];
    this._callback = options.callback || function(){};
    this._MAX_CSETS = options.max_csets || 500;
    this._MAX_TRIPLES = options.max_triples || 350000;
    this._triples = 0;
    this._csets = 0;
    this._found = {'added': function(triple,store){}, 
                   'removed': function(triple,store) { store.removeTriple(triple);}},
    this._not_found = {'added': function(triple,store){ store.addTriple(triple);},
                       'removed': function(triple,store){}};
};

OperationList.prototype.computeOperationList = function(cLists,start) {
    console.log("Computing the operation list. Pos: "+start+" | Len: "+cLists.length);
    start = start || 0;
    if(cLists.length <= start ||
       cLists[start].csets === undefined) {
        console.log("Reached the end of the list!");
        this._callback(this.opList);
        return;
    }
    var _this = this;
    setImmediate(function(){_this._operationsFromCsetList(cLists,cLists[start].csets,start);});
};
OperationList.prototype._operationsFromCsetList = function(cLists,csList,csListPos) {
    var _this = this;
    if(!csList || csList.length === 0) {
        // This changeset list does not contain any changesets. We'll go on to the next one
        console.log("Empty cSet list. Len: "+csList.length);
        setImmediate(function(){_this.computeOperationList(cLists,csListPos+1);});
        return;
    }
    console.log("Computing ops from CSetList. Pos: "+csListPos+" | Len: "+csList.length);
    setImmediate(function(){_this._operationsFromCset(cLists,csList,csList[0],csListPos,0);});
};
OperationList.prototype._operationsFromCset = function(cLists,csList,cSet,csListPos,cSetPos) {
    var _this = this;
    if(csList.length <= cSetPos) {
        console.log("Covered whole csList. CSts: "+this._csets+" | TRps: "+this._triples+
                    " | CSPos: "+cSetPos+" | LLen: "+csList.length);
        setImmediate(function(){_this.computeOperationList(cLists,csListPos+1);});
        return;
    }
    var triples = cSet.getTriples(),
        operation = cSet.getOperation();

    for(var j = 0; j < triples.length; j++) {
        var tr = triples[j];
        if(this.opStore.find(tr.subject, tr.predicate, tr.object).length > 0) {
            this._found[operation](tr,this.opStore);
        } else {
            this._not_found[operation](tr,this.opStore);
            this.opList.push({operation: operation, triple: tr});
        }
    }
    this._triples += triples.length;
    this._csets += 1;
    if(this._triples >= this._MAX_TRIPLES || this._csets >= this._MAX_CSETS) {
        // We have reached the maximum number of triples or changesets. We finalize.
        console.log("Reached max number of triples or changesets. We end."+
                    "CSPos:"+cSetPos+
                    "|TR:"+this._triples+"|CS:"+this._csets);
        this._callback(this.opList);
        return;
    }
    setImmediate(function() { _this._operationsFromCset(cLists,csList,csList[cSetPos+1],csListPos,cSetPos+1);});
};

module.exports = OperationList;
