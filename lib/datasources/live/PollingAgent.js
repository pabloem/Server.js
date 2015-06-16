/** A PollingAgent controls how often the LDF server should poll for live updates. */

var fs = require('fs'),
    ChangesetManager = require('./ChangesetManager.js');


function PollingAgent(options) {
    options = options || {};
    // The pollingInterval option is how often -in minutes-, we should
    // poll the server for new changesets. If there's no such option, then
    // we query every 5 minutes, and let ChangesetManager decide if we'll
    // go ahead and commit to the Datasource
    this.pollingInterval = options.pollingInterval || 60;
    this._pollCounter = 0;
}

PollingAgent.prototype.startPolling = function() {
    var _this = this;
    this._intervalObj = setInterval(function() {_this.pollCounting();}, 1000*60);
};
PollingAgent.prototype.stopPolling = function() {
    if(this._intervalObj) {
        clearInterval(this._intervalObj);
    }
};

PollingAgent.prototype.pollCounting = function() {
    this._pollCounter += 1;
    if(this._pollCounter >= this.pollingInterval){
        this._pollCounter = 0;
        this.pollServer();
    }
};

PollingAgent.prototype.pollServer = function() {
    this._csManager.checkForChangesets();
};

module.exports = PollingAgent;
