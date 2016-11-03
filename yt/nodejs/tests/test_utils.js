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

describe("#bestAcceptedType()", function() {
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
            expect(utils.bestAcceptedType([ mime ], header)).to.eql(mime);
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
            expect(utils.bestAcceptedType([ mime ], header)).to.be.undefined;
        });
    });

    it("should work with real header", function() {
        var good_header = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
        var bad_header = "text/html,application/xhtml+xml,application/xml;q=0.9";
        expect(utils.bestAcceptedType(
            [ "text/html", "application/json" ], good_header
        )).to.eql("text/html");
        expect(utils.bestAcceptedType(
            [ "application/json" ], good_header
        )).to.eql("application/json");
        expect(utils.bestAcceptedType(
            [ "application/xxxx" ], bad_header
        )).to.be.undefined;
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
            expect(utils.bestAcceptedEncoding([ mime ], header)).to.eql(mime);
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
            expect(utils.bestAcceptedEncoding([ mime ], header)).to.be.undefined;
        });
    });
});

describe("#parseAcceptType()", function() {
    it("should parse real header", function() {
        expect(
            utils.parseAcceptType("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        ).to.eql([
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
        ).to.eql([
            { value: "gzip", quality: 1.0 },
            { value: "deflate", quality: 1.0 },
            { value: "sdch", quality: 1.0 }
        ]);
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

var TIME_PAIRS = [
    {
        isoShort: "2016-01-02T19:34:31.712507Z",
        isoLong:  "2016-01-02T19:34:31.712507Z",
        microseconds: 1451763271712507,
    },
    {
        isoShort: "2016-01-02T19:34:31.712Z",
        isoLong:  "2016-01-02T19:34:31.712000Z",
        microseconds: 1451763271712000,
    },
    {
        isoShort: "2016-01-02T19:34:31Z",
        isoLong:  "2016-01-02T19:34:31.000000Z",
        microseconds: 1451763271000000,
    },
];

describe("#microsToUtcString()", function() {
    it("should should yield correct ISO date with microseconds precision", function() {
        TIME_PAIRS.forEach(function(time) {
            expect(utils.microsToUtcString(time.microseconds)).to.equal(time.isoLong);
        });
    });
});

describe("#utcStringToMicros()", function() {
    it("should should yield correct number of microseconds", function() {
        TIME_PAIRS.forEach(function(time) {
            expect(utils.utcStringToMicros(time.isoShort)).to.equal(time.microseconds);
            expect(utils.utcStringToMicros(time.isoLong)).to.equal(time.microseconds);
        });
    });
});

describe("#gather", function() {
    it("should return singleton item", function() {
        expect(utils.gather({"k": "v"}, "k")).to.eql("v");
    });

    it("should return null when there is no item", function() {
        expect(utils.gather({"k": "v"}, "l")).to.eql(null);
    });

    it("should return single part array", function() {
        expect(utils.gather({"k0": "v"}, "k")).to.deep.equal(["v"]);
    });

    it("should return multi part array", function() {
        expect(utils.gather({"k0": "v", "k1": "w"}, "k")).to.deep.equal(["v", "w"]);
        expect(utils.gather({"k1": "w", "k0": "v"}, "k")).to.deep.equal(["v", "w"]);
    });

    it("should fail on missing parts", function() {
        expect(function() { utils.gather({"k0": "v", "k2": "w"}, "k"); }).to.throw();
    });

    it("should fail on bad parts", function() {
        expect(function() { utils.gather({"kk": "v"}, "k"); }).to.throw();
    });
});

// Impicit scope.
});
