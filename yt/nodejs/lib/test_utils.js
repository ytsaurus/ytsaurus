var expect = require("chai").expect;
var assert = require("chai").assert;

var utils  = require("./utils");

////////////////////////////////////////////////////////////////////////////////

describe("http utilities - .parseQuality()", function() {
    it("should parse entry without q-value", function() {
        var result = utils.parseQuality("text/html");
        expect(result.value).to.be.equal("text/html");
        expect(result.quality).to.be.equal(1.0);
    });

    it("should parse entry with q=0.5", function() {
        var result = utils.parseQuality("text/html;q=0.5");
        expect(result.value).to.be.equal("text/html");
        expect(result.quality).to.be.equal(0.5);
    });

    it("should parse entry with q=.5", function() {
        var result = utils.parseQuality("text/html;q=.5");
        expect(result.value).to.be.equal("text/html");
        expect(result.quality).to.be.equal(0.5);
    });
});

describe("http utilities - .parseAccept()", function() {
    it("should parse real header", function() {
        expect(
            utils.parseAccept("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        ).to.be.eql([
            { value: "text/html", quality: 1.0, type: "text", subtype: "html" },
            { value: "application/xhtml+xml", quality: 1.0, type: "application", subtype: "xhtml+xml" },
            { value: "application/xml", quality: 0.9, type: "application", subtype: "xml" },
            { value: "*/*", quality: 0.8, type: "*", subtype: "*" }
        ]);
    });
});

describe("http utilities - .accepts()", function() {
    it("should correctly deduce output format from Accept header", function() {
        expect(utils.accepts(
            "application/json",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        )).to.be.true;
        expect(utils.accepts(
            "application/xxxx",
            "text/html,application/xhtml+xml,application/xml;q=0.9"
        )).to.be.false;
    });
});

describe("http utilities - .is()", function() {
    it("should match full type", function() {
        expect(utils.is(
            "application/json",
            "application/json"
        )).to.be.true;

        expect(utils.is(
            "application/xxxx",
            "application/json"
        )).to.be.false;
    });

    it("should ignore additional information in the header", function() {
        expect(utils.is(
            "text/html",
            "text/html; charset=utf-8"
        )).to.be.true;
    });

    it("should support asterisks", function() {
        expect(utils.is("*/html", "text/html")).to.be.true;
        expect(utils.is("text/*", "text/html")).to.be.true;
        expect(utils.is("*/*",    "text/html")).to.be.true;
    });
});

describe("numerify", function() {
    it("should preserve undefined", function() {
        expect(utils.numerify(undefined)).to.be.undefined;
    });

    it("should preserve null", function() {
        expect(utils.numerify(null)).to.be.null;
    });

    it("should preserve non-numeric strings", function() {
        expect(utils.numerify("foobar")).to.be.equal("foobar");
        expect(utils.numerify("1000foobar")).to.be.equal("1000foobar");
    });

    it("should numerify numeric strings", function() {
        expect(utils.numerify("1000")).to.be.equal(1000);
    });

    it("should work recursively on maps", function() {
        expect(utils.numerify({ a: { b : { c : "100" }, d : "10" }, e : "1" }))
            .to.be.deep.equal({ a: { b : { c : 100 }, d : 10 }, e : 1 });
    });

    it("should work recursively on lists", function() {
        expect(utils.numerify([ "1", [ "2", "3", [ "4" ]]]))
            .to.be.deep.equal([ 1, [ 2, 3, [ 4 ]]]);
    });
});
