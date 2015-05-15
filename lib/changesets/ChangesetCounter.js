var util = require('util');

// Method: ChangesetCounter
// Input: A string of the form 'year/month/day/hour/count', a 
//  dictionary of the form {year:2015,month:1,day:1,hour:0,count:1},
//  or a list [year,month,day,hour,count].
// Returns: A ChangesetCounter object.
function ChangesetCounter(input) {
    if(Array.isArray(input)) {
        input = input.slice();
        // Adding data that might not have been provided (hours & count)
        while(input.length < 5){ input.push(0); }

        this._year = input[0];
        this._month = input[1];
        this._day = input[2];
        this._hour = input[3];
        this._count = input[4];
    } else if(typeof(input) == "object") {
        this._year = input.year;
        this._month = input.month;
        this._day = input.day;
        this._hour = input.hour || 0;
        this._count = input.count || 0;
    } else if (typeof(input) == "string") {
        var data = input.split("/");
        // Adding data that might not have been provided (hours & count)
        while(data.length < 5){ data.push("0"); }

        this._year = parseInt(data[0]);
        this._month = parseInt(data[1]);
        this._day = parseInt(data[2]);
        this._hour = parseInt(data[3]);
        this._count = parseInt(data[4]);
    }
};

ChangesetCounter.prototype.isConsistent = function() {
    if(this._hour < 0 || this._hour > 23) return false;
    if(this._day < 1 || this._day > 31) return false;
    if(this._month < 1 || this._month > 12) return false;
    if(this._year < 0) return false;
    if(this._count < 0) return false;
    return true;
};

// This method zero-pads integers to the left
ChangesetCounter.prototype._zeroPad = function(number,size) {
    number = number.toString();
  while (number.length < size) number = "0" + number;
  return number;
};

ChangesetCounter.prototype.getPath = function() {
    return this._zeroPad(this._year,4) + "/" +
        this._zeroPad(this._month,2) + "/" +
        this._zeroPad(this._day,2) + "/" +
        this._zeroPad(this._hour,2) + "/" +
        this._zeroPad(this._count,6) + "/";
};

// Methods: nextChangeset and zeroChangeset
// Result: They increment, or set to zero the _count variable, respectively.
// Return: Nothing.
ChangesetCounter.prototype.nextChangeset = function() {
    this._count += 1;
};
ChangesetCounter.prototype.zeroChangeset = function() {
    this._count = 0;
};

// Method: _setDefaultInitialVal
// Result: Sets the initial value for this property. 
// Returns: Nothing
ChangesetCounter.prototype._setDefaultInitialVal = function(property) {
// It's either 1 or 0. If 0 breaks consistency, then it's 1.
    this[property] = 0;
    if(!this.isConsistent()) {
        this[property] = 1;
    }
    return;
};

// Method: nextHour
// This method is a bit involved. Might want to make it into a clearer
// version.
// Result: Sets the ChangesetCounter to the next Hour.
// Returns: Nothing
ChangesetCounter.prototype.nextHour = function() {
    // If the current state is not consistent, we can't increment
    if(!this.isConsistent()) return;

    var increments = ["_hour","_day","_month","_year"],
        inc_idx = 0;

    var done = false;
    while(!done) {
        this[increments[inc_idx]] += 1;
        if(!this.isConsistent()) {
            this._setDefaultInitialVal(increments[inc_idx]);
            inc_idx += 1;

            // We incremented everything we could, and failed
            if(inc_idx >= increments.length) return;
        } else {
            done = true;
        }
    }
};

module.exports = ChangesetCounter;
