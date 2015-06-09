// Method: ChangesetCounter
// Input: A string of the form 'year/month/day/hour/count', a 
//  dictionary of the form {year:2015,month:1,day:1,hour:0,count:1},
//  or a list [year,month,day,hour,count].
// Returns: A ChangesetCounter object.
function ChangesetCounter(input) {
    var date = new Date();
    date.setMinutes(0);
    date.setSeconds(0);
    date.setMilliseconds(0);
    this._count = 0;
    this._date = date;

    if(typeof(input) === 'undefined') {
        return; // We use the current date.
    } else if (typeof(input) == "string") {
        input = input.split("/");
    }

    date.setYear(parseInt(input.year || input[0]) || 0);
    date.setMonth((parseInt(input.month || input[1]) -1) || 0);
    date.setDate(parseInt(input.day || input[2]) || 0);
    date.setHours(parseInt(input.hour || input[3]) || 0);
    this._count = parseInt(input.count || input[4]) || 0;
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
