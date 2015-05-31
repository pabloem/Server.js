var util = require('util');

// Method: ChangesetCounter
// Input: A string of the form 'year/month/day/hour/count', a 
//  dictionary of the form {year:2015,month:1,day:1,hour:0,count:1},
//  or a list [year,month,day,hour,count].
// Returns: A ChangesetCounter object.
function ChangesetCounter(input) {
    var _year,_month,_day,_hour,_count;
    this._date = new Date();
    this._date.setMinutes(0);
    this._date.setSeconds(0);
    this._date.setMilliseconds(0);
    this._count = 0;

    if(typeof(input) === 'undefined') {
        return; // We use the current date.
    } else if(Array.isArray(input)) {
        input = input.slice();
        // Adding data that might not have been provided (hours & count)
        while(input.length < 5){ input.push(0); }
        _year = input[0];
        _month = input[1];
        _day = input[2];
        _hour = input[3];
        _count = input[4];
    } else if(typeof(input) == "object") {
        _year = input.year;
        _month = input.month;
        _day = input.day;
        _hour = input.hour || 0;
        _count = input.count || 0;
    } else if (typeof(input) == "string") {
        var data = input.split("/");
        // Adding data that might not have been provided (hours & count)
        while(data.length < 5){ data.push("0"); }

        _year = parseInt(data[0]);
        _month = parseInt(data[1]);
        _day = parseInt(data[2]);
        _hour = parseInt(data[3]);
        _count = parseInt(data[4]);
    }
    this._date.setYear(_year);
    this._date.setMonth(_month-1);
    this._date.setDate(_day);
    this._date.setHours(_hour);
    this._count = _count;
}

ChangesetCounter.prototype.isHourEqual = function(cc) {
    if(this._date.getTime() == cc._date.getTime()) return true;
    return false;
};
ChangesetCounter.prototype.isSmallerOrEqual = function(cc) {
    if(this._date.getTime() < cc._date.getTime() || 
       (this._date.getTime() == cc._date.getTime() && cc._count !== 0 &&
        this._count <= cc._count) ||
       (this._date.getTime() == cc._date.getTime() && cc._count === 0) // We consider 0 counts as no-counts
      ) {
        return true;
    }
    return false;
};

// This method zero-pads integers to the left
ChangesetCounter.prototype._zeroPad = function(number,size) {
    number = number.toString();
  while (number.length < size) number = "0" + number;
  return number;
};

ChangesetCounter.prototype.getPath = function() {
    return this.getHourPath() +
        this._zeroPad(this._count,6);
};

ChangesetCounter.prototype.getHourPath = function() {
    return this._zeroPad(this._date.getFullYear(),4) + "/" +
        this._zeroPad(this._date.getMonth()+1,2) + "/" +
        this._zeroPad(this._date.getDate(),2) + "/" +
        this._zeroPad(this._date.getHours(),2) +"/";
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
ChangesetCounter.prototype.getCount = function() {
    return this._count || 0;
};
ChangesetCounter.prototype.setCount = function(input) {
    this._count = input || 0;
};
// Method: nextHour
// Result: Sets the ChangesetCounter to the next Hour. Resets _count to zero.
// Returns: Nothing
ChangesetCounter.prototype.nextHour = function() {
    this._date.setHours(this._date.getHours()+1);
    this._count = 0;
};

module.exports = ChangesetCounter;
