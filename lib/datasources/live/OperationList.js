var N3 = require('n3');
var OperationList = function(options){
    options = options || {};
    this._stores = {'added':N3.Store(), 'removed':N3.Store()};
    
    this._callback = options.callback || function(){};
    this._MAX_CSETS = options.max_csets || 500;
    this._MAX_TRIPLES = options.max_triples || 350000;
    this._triples = 0;
    this._csets = 0;
};

OperationList.prototype.finalize = function(cListsPos,cSetPos) {
    console.log("FInalize: cListPos: "+cListsPos+" | cSetPos: "+cSetPos);
    this.added = this._stores['added'].find();
    this.removed = this._stores['removed'].find();
    this.csListsIdx = cListsPos;
    this.cSetPos = cSetPos;
    delete this._stores;
    this._callback();
};

OperationList.prototype.computeOperationList = function(cLists,start) {
    start = start || 0;
    console.log("Computing the operation list. Pos: "+start+" | Len: "+cLists.length);
    if(cLists.length <= start ||
       cLists[start].csets === undefined) {
        console.log("Reached the end of the list!");
        this.finalize(start);
        return;
    }
    var _this = this;
    setImmediate(function(){_this._operationsFromCsetList(cLists,cLists[start].csets,start);});
};
OperationList.prototype._operationsFromCsetList = function(cLists,csList,csListsPos) {
    var _this = this;
    if(!csList || csList.length === 0) {
        // This changeset list does not contain any changesets. We'll go on to the next one
        console.log("Empty cSet list. Len: "+csList.length);
        setImmediate(function(){_this.computeOperationList(cLists,csListsPos+1);});
        return;
    }
    console.log("Computing ops from CSetList. Pos: "+csListsPos+" | Len: "+csList.length);
    setImmediate(function(){_this._operationsFromCset(cLists,csList,csList[0],csListsPos,0);});
};
OperationList.prototype._operationsFromCset = function(cLists,csList,cSet,csListsPos,cSetPos) {
    var _this = this;
    if(csList.length <= cSetPos) {
        console.log("Covered whole csList. CSts: "+this._csets+" | TRps: "+this._triples+
                    " | CSPos: "+cSetPos+" | LLen: "+csList.length);
        setImmediate(function(){_this.computeOperationList(cLists,csListsPos+1);});
        return;
    }
    var triples = cSet.getTriples(),
        operation = cSet.getOperation(),
        oppositeOp = operation == 'added' ? 'removed' : 'added';

    for(var j = 0; j < triples.length; j++) {
        var tr = triples[j];
        if(this._stores[oppositeOp].find(tr.subject, tr.predicate, tr.object).length > 0) {
            this._stores[oppositeOp].removeTriple(tr);
        } else {
            this._stores[operation].addTriple(tr);
        }
    }
    this._triples += triples.length;
    this._csets += 1;
    if(this._triples >= this._MAX_TRIPLES || this._csets >= this._MAX_CSETS) {
        // We have reached the maximum number of triples or changesets. We finalize.
        console.log("Reached max number of triples or changesets. "+
                    "CSPos:"+cSetPos+
                    "|TR:"+this._triples+"|CS:"+this._csets);
        this.finalize(csListsPos,cSetPos+1);
        return;
    }
    setImmediate(function() { _this._operationsFromCset(cLists,csList,csList[cSetPos+1],csListsPos,cSetPos+1);});
};

module.exports = OperationList;
