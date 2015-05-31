function PollingAgent(options) {
    // The pollingInterval option is how often -in minutes-, we should
    // poll the server for new changesets. If there's no such option, then
    // we query every 5 minutes, and let ChangesetManager decide if we'll
    // go ahead and commit to the Datasource
    this.pollingInterval = options.pollingInterval || 5;
    this._pollCounter = 0;
    this._intervalObj = setInterval(function() {this.pollingCounting();}, 1000*60);

    var ChangesetManager = require(options.csetManager || './ChangesetManager.js');
    this._csManager = new ChangesetManager(options);
}

PollingAgent.prototype.pollingCounting = function() {
    this._pollCounter += 1;
    if(this._pollCounter >= this.pollingInterval){
        this._pollCounter = 0;
        this.pollServer();
    }
};

PollingAgent.prototype.pollServer = function() {
    this._csManager.checkForChangesets();
};
