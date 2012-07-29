var utils  = require("../lib/utils");

////////////////////////////////////////////////////////////////////////////////

describe("http utilities", function() {
// Impicit scope.

describe("#matches()", function() {
    it("should match full type", function() {
        expect(utils.matches(
            "application/json",
            "application/json"
        )).to.be.true;

        expect(utils.matches(
            "application/xxxx",
            "application/json"
        )).to.be.false;
    });

    it("should ignore additional information in the header", function() {
        expect(utils.matches(
            "text/html",
            "text/html; charset=utf-8"
        )).to.be.true;
    });

    it("should support asterisks", function() {
        utils.matches("*/html", "text/html").should.be.true;
        utils.matches("*/html", "text/xxxx").should.be.false;
        utils.matches("text/*", "text/html").should.be.true;
        utils.matches("text/*", "xxxx/html").should.be.false;
        utils.matches("*/*",    "text/html").should.be.true;
    });

    it("should not fail on invalid inputs", function() {
        utils.matches("x", "x").should.be.true;
        utils.matches("x", "y").should.be.false;
    });
});

describe("#acceptsType()", function() {
    // True cases.
    [
        [ "text/plain", "text/plain" ],
        [ "text/plain", "*/plain"    ],
        [ "text/plain", "text/*"     ],
        [ "text/plain", "*/*"        ],
    ].forEach(function(pair) {
        var mime = pair[0];
        var header = pair[1];

        it("should be true for ('" + mime + "', '" + header + "')", function() {
            utils.acceptsType(mime, header).should.be.true;
        });
    });

    // False cases.
    [
        [ "text/plain", "application/json" ],
        [ "text/plain", "*/json"           ],
        [ "text/plain", "application/*"    ],
    ].forEach(function(pair) {
        var mime = pair[0];
        var header = pair[1];

        it("should be false for ('" + mime + "', '" + header + "')", function() {
            utils.acceptsType(mime, header).should.be.false;
        });
    });

    it("should work with real header", function() {
        expect(utils.acceptsType(
            "application/json",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        )).to.be.true;
        expect(utils.acceptsType(
            "application/xxxx",
            "text/html,application/xhtml+xml,application/xml;q=0.9"
        )).to.be.false;
    });
});

describe("#acceptsEncoding()", function() {
    // True cases.
    [
        [ "gzip",     "gzip"              ],
        [ "gzip",     "gzip,deflate,sdch" ],
        [ "deflate",  "gzip,deflate,sdch" ],
        [ "sdch",     "gzip,deflate,sdch" ],
        [ "identity", "*"                 ],
    ].forEach(function(pair) {
        var mime = pair[0];
        var header = pair[1];

        it("should be true for ('" + mime + "', '" + header + "')", function() {
            utils.acceptsEncoding(mime, header).should.be.true;
        });
    });

    // False cases.
    [
        [ "gzip",     "identity"          ],
        [ "identity", "gzip,deflate,sdch" ],
    ].forEach(function(pair) {
        var mime = pair[0];
        var header = pair[1];

        it("should be false for ('" + mime + "', '" + header + "')", function() {
            utils.acceptsEncoding(mime, header).should.be.false;
        });
    });
});

describe("#parseQuality()", function() {
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

describe("#parseAcceptType()", function() {
    it("should parse real header", function() {
        expect(
            utils.parseAcceptType("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        ).to.be.eql([
            { value: "text/html", quality: 1.0, type: "text", subtype: "html" },
            { value: "application/xhtml+xml", quality: 1.0, type: "application", subtype: "xhtml+xml" },
            { value: "application/xml", quality: 0.9, type: "application", subtype: "xml" },
            { value: "*/*", quality: 0.8, type: "*", subtype: "*" }
        ]);
    });
});

describe("#parseAcceptEncoding()", function() {
    it("should parse real header", function() {
        expect(
            utils.parseAcceptEncoding("gzip,deflate,sdch")
        ).to.be.eql([
            { value: "gzip", quality: 1.0 },
            { value: "deflate", quality: 1.0 },
            { value: "sdch", quality: 1.0 }
        ]);
    });
});

describe("#numerify()", function() {
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

// Impicit scope.
});
