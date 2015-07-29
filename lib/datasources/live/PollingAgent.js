/** A PollingAgent controls how often the LDF server should poll for live updates. */

var fs = require('fs'),
    ChangesetManager = require('./ChangesetManager.js');


function PollingAgent(options,applyCsetCallback) {
  options = options || {};
  // The pollingInterval option is how often -in minutes-, we should
  // poll the server for new changesets. If there's no such option, then
  // we query every 5 minutes, and let ChangesetManager decide if we'll
  // go ahead and commit to the Datasource
  this.pollingInterval = options.pollingInterval || 60;
  this._pollCounter = 0;

  this._regeneratingInterval = options.regeneratingInterval || 2880; // Regenerate the file every two days by default
  this._regenCounter = 0;
  this._csManager = new ChangesetManager(options,applyCsetCallback);
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
  this._regenCounter += 1;
  console.log("1 Minute...");
  if(this._regenCounter >= this._regeneratingInterval) {
    var _this = this,
        afterRegenerate = function(){ _this.startPolling(); };
    this.stopPolling(); // We stop polling until regenerating the file
    // Regenerate file
    this._regenCounter = 0;
    return ;
  }
  if(this._pollCounter >= this.pollingInterval){
    console.log("Fetching!");
    this._pollCounter = 0;
    this.pollServer();
  }
};

PollingAgent.prototype.pollServer = function() {
  var _this = this,
      afterRegenerate = function(){ console.log("Resuming polling!"); _this.startPolling(); };
  this.stopPolling(); // We stop polling until adding all changesets
  this._csManager.checkForChangesets(afterRegenerate);
};

module.exports = PollingAgent;
