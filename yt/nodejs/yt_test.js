var yt = require('./build/Release/yt_test');

var expect = require('chai').expect;
var assert = require('chai').assert;

describe("interface to streams", function() {
    before(function() {
        this.topic = new yt.DriverHost();
    })
    it("should work in case 1", function() {
        this.topic.write_to_input(new Buffer("foo"), 0, 3);
        this.topic.write_to_input(new Buffer("bar"), 0, 3);

        this.topic.test(1);
        this.topic.test(1);
        this.topic.test(1);
        this.topic.test(1);
        this.topic.test(1);
        this.topic.test(1);
    });

    it("should work in case 2", function() {
        this.topic.write_to_input(new Buffer("foo"), 0, 3);
        this.topic.write_to_input(new Buffer("bar"), 0, 3);

        setTimeout(function() {
            this.topic.write_to_input(new Buffer("12345"), 0, 5);
        }, 100);
        setTimeout(function() {
            this.topic.close_input();
        }, 200);

        this.topic.test(3);
        this.topic.test(3);

        this.topic.test(4);
        this.topic.test(4);
        this.topic.test(4);
        this.topic.test(4);
        this.topic.test(4);
    });

    it("should work in case 3", function() {
        this.topic.write_to_input(new Buffer("12345"), 0, 5);
        this.topic.close_input();

        this.topic.test(3);
        this.topic.test(3);
        this.topic.test(3);
    });
});
