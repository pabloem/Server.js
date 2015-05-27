var ChangesetCounter = require('../../lib/changesets/ChangesetCounter');

describe('ChangesetCounter', function() {
    describe('A ChangesetCounter instance', function() {
        var cc = new ChangesetCounter(),
            date = new Date();
        it('if created without arguments, it should be of the current time', function() {
            var nums = cc.getPath().split("/");
            date.getFullYear().should.equal(parseInt(nums[0]));
            (date.getMonth()+1).should.equal(parseInt(nums[1]));
            date.getDate().should.equal(parseInt(nums[2]));
            date.getHours().should.equal(parseInt(nums[3]));
            (0).should.equal(parseInt(nums[4]));
        });
        it('should increment count properly', function() {
            cc.nextChangeset();
            var nums = cc.getPath().split("/");
            date.getFullYear().should.equal(parseInt(nums[0]));
            (date.getMonth()+1).should.equal(parseInt(nums[1]));
            date.getDate().should.equal(parseInt(nums[2]));
            date.getHours().should.equal(parseInt(nums[3]));
            (1).should.equal(parseInt(nums[4]));
        });
        it('should reset count after incrementing hour', function() {
            cc.nextHour();
            date.setHours(date.getHours()+1);
            var nums = cc.getPath().split("/");
            (0).should.equal(parseInt(nums[4]));
        });
        it('should increment hour consistently with calendar', function() {
            cc.nextHour();
            date.setHours(date.getHours()+1);
            var nums = cc.getPath().split("/");
            date.getFullYear().should.equal(parseInt(nums[0]));
            (date.getMonth()+1).should.equal(parseInt(nums[1]));
            date.getDate().should.equal(parseInt(nums[2]));
            date.getHours().should.equal(parseInt(nums[3]));
            (0).should.equal(parseInt(nums[4]));
        });
        it('should set count freely', function() {
            var value = date.getMilliseconds(); // Number at random
            cc.setCount(value);
            var nums = cc.getPath().split("/");
            value.should.equal(parseInt(nums[4]));
        });
    });
    describe('Two ChangesetCounter instances', function() {
        var cca = new ChangesetCounter(),
            ccb = new ChangesetCounter();
        it('should be equal if created without arguments at the same time', function(){
            cca.isSmallerOrEqual(ccb).should.equal(true);
            ccb.isSmallerOrEqual(cca).should.equal(true);
        });
        it('should be considered equal if hour is equal, and any of their counts is zero', function() {
            cca.setCount(10);
            cca.isSmallerOrEqual(ccb).should.equal(true);
            ccb.isSmallerOrEqual(cca).should.equal(true);
        });
        it(',if both counts are nonzero, the one with smaller count should be considered smaller', function() {
            ccb.setCount(1);
            cca.isSmallerOrEqual(ccb).should.equal(false);
            ccb.isSmallerOrEqual(cca).should.equal(true);
        });
        it('are hour-equal if their hours are the same, no matter what their counts', function() {
            cca.isHourEqual(ccb).should.equal(true);
            ccb.isHourEqual(cca).should.equal(true);
        });
        it(', if their hours are different, the one with smaller hour is smaller', function() {
            cca.nextHour();
            ccb.isSmallerOrEqual(cca).should.equal(true);
            cca.isSmallerOrEqual(ccb).should.equal(false);
        });
    });
});
