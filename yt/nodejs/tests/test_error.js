var binding = require("../lib/ytnode");
process.on("exit", binding.ShutdownSingletons);

var YtError = require("../lib/error").that;

////////////////////////////////////////////////////////////////////////////////

var makeError = function(code, message, attributes) {
    return binding.SpawnBasicYtError(
        code,
        message,
        binding.CreateV8Node(attributes));
};

describe("native errors", function() {
    it("should properly convert from C++ errors to V8 errors", function() {
        var err = makeError(42, "The meaning of life", { a : 3, b : 4 });
        err.code.should.eql(42);
        err.message.should.eql("The meaning of life");
        err.attributes.a.should.eql("3");
        err.attributes.b.should.eql("4");
    });

    it("should provide JSON-serialized attributes", function() {
        var err = makeError(42, "QWERTY", {
            a : { x : 1, y : "foo", z : [ "bar", "baz"] } 
        });
        [
            '{"x":1,"y":"foo","z":["bar","baz"]}',
            '{"x":1,"z":["bar","baz"],"y":"foo"}',
            '{"y":"foo","x":1,"z":["bar","baz"]}',
            '{"y":"foo","z":["bar","baz"],"x":1}',
            '{"z":["bar","baz"],"x":1,"y":"foo"}',
            '{"z":["bar","baz"],"y":"foo","x":1}'
        ].should.include(err.attributes.a);
    });

    it("should derive from YtError", function() {
        var err = makeError(42, "QWERTY", {});
        err.should.be.instanceof(YtError);
    });
});

describe("wrapped errors", function() {
    it("should derive from YtError", function() {
        var err = new YtError();

        err.should.be.instanceof(YtError);
    });

    it("should construct OK with no arguments", function() {
        var err = new YtError();

        err.should.be.instanceof(YtError);
        err.code.should.eql(0);
        err.message.should.be.empty;
        err.attributes.should.be.empty;
        err.inner_errors.should.be.empty;
    });

    it("should construct from string", function() {
        var err = new YtError("A strange error");

        err.should.be.instanceof(YtError);
        err.code.should.not.eql(0);
        err.message.should.eql("A strange error");
        err.attributes.should.be.empty;
        err.inner_errors.should.be.empty;
    });

    it("should construct from JS error", function() {
        var err = new YtError(new Error("QWERTY"));

        err.should.be.instanceof(YtError);
        err.code.should.not.eql(0);
        err.message.should.eql("QWERTY");
        err.attributes.should.have.keys("stack");
        err.inner_errors.should.be.empty;
    });

    it("should construct from native error", function() {
        var err = new YtError(makeError(42, "QWERTY", { buzz : 17 }));

        err.should.be.instanceof(YtError);
        err.code.should.eql(42);
        err.message.should.eql("QWERTY");
        err.attributes.should.have.keys("buzz");
        err.inner_errors.should.be.empty;
    });

    it("should construct nested from string and JS error", function() {
        var err = new YtError(
            "A strange error",
            new Error("QWERTY"));

        err.should.be.instanceof(YtError);
        err.code.should.not.eql(0);
        err.message.should.eql("A strange error");
        err.attributes.should.be.empty;
        err.inner_errors.should.have.length(1);

        var inner = err.inner_errors[0];
        inner.should.be.instanceof(YtError);
        inner.code.should.not.eql(0);
        inner.message.should.eql("QWERTY");
        inner.attributes.should.have.keys("stack");
        inner.inner_errors.should.be.empty;
    });

    it("should construct nested from string and native error", function() {
        var err = new YtError(
            "A strange error",
            makeError(42, "QWERTY", {}));

        err.should.be.instanceof(YtError);
        err.code.should.not.eql(0);
        err.message.should.eql("A strange error");
        err.attributes.should.be.empty;
        err.inner_errors.should.have.length(1);

        var inner = err.inner_errors[0];
        inner.should.be.instanceof(YtError);
        inner.code.should.eql(42);
        inner.message.should.eql("QWERTY");
        inner.attributes.should.be.empty;
        inner.inner_errors.should.be.empty;
    });

    it("should find unavailable codes", function() {
        var err1 = makeError(binding.UnavailableYtErrorCode, "Foo", {});
        var err2 = new YtError("Bar", err1);
        var err3 = new YtError("Baz");

        err1.isUnavailable().should.be.true;
        err2.isUnavailable().should.be.true;
        err3.isUnavailable().should.be.false;
    });
});

describe("error serialization", function() {
    it("should expose meaningful toString()", function() {
        var err = makeError(42, "QWERTY", {});
        err.toString().should.eql("YtError: QWERTY");
    });

    it("should serialize simple case", function() {
        var err = makeError(42, "QWERTY", {});
        err.toJson().should.eql('{' +
            '"code":42,' +
            '"message":"QWERTY",' +
            '"attributes":{},' +
            '"inner_errors":[]' +
            '}');
    });

    it("should serialize attributes", function() {
        var err;
        err = makeError(42, "The meaning of life",
            { a : 3, b : 4 });
        err.toJson().should.eql('{' +
            '"code":42,' +
            '"message":"The meaning of life",' +
            '"attributes":{"a":3,"b":4},' +
            '"inner_errors":[]' +
            '}');
        err = makeError(42, "QWERTY",
            { a : { x : 1, y : "foo", z : [ "bar", "baz"] } });
        err.toJson().should.eql('{' +
            '"code":42,' +
            '"message":"QWERTY",' +
            '"attributes":{"a":{"z":["bar","baz"],"x":1,"y":"foo"}},' +
            '"inner_errors":[]' +
            '}');
    });
});
