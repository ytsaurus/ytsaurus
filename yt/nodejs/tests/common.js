// Register at-exit callback.
var binding = require("../lib/ytnode");

global.sinon = require("sinon");
global.chai = require("chai");

global.should = chai.should();
global.expect = chai.expect;

if (process.env.NODE_DEBUG && /YTTEST/.test(process.env.NODE_DEBUG)) {
    global.__DBG = function(x) {
        "use strict";
        process.stderr.write("__DBG : ");
        console.error(x);
    };
    global.__LOG = function() {
        "use strict";
        process.stderr.write("__LOG : ");
        console.error.apply(null, arguments);
    };
} else {
    global.__DBG = function(){};
    global.__LOG = function(){};
}

global.stubLogger = function(callback) {
    var sink = callback || __LOG;
    var stub = {};

    [ "info", "warn", "debug", "error" ].forEach(function(level) {
        stub[level] = sink;
    });

    return stub;
};

global.HTTP_PORT = 0;
global.HTTP_HOST = "127.0.0.1";
global.HTTP_LAG  = 5;

var sinonChai = require("sinon-chai");
chai.use(sinonChai);

// A bunch of helpful assertions to use while testing HTTP.

chai.Assertion.addProperty("http2xx", function() {
    this._obj.statusCode.should.be.within(200, 300);
});

chai.Assertion.addProperty("http3xx", function() {
    this._obj.statusCode.should.be.within(300, 400);
});

chai.Assertion.addProperty("http4xx", function() {
    this._obj.statusCode.should.be.within(400, 500);
});

chai.Assertion.addProperty("http5xx", function() {
    this._obj.statusCode.should.be.within(500, 600);
});

chai.Assertion.addMethod("content_disposition", function(disposition) {
    if (disposition) {
        this._obj.headers["content-disposition"].should.eql(disposition);
    } else {
        this._obj.headers["content-disposition"].should.be.a("string");
    }
});

chai.Assertion.addMethod("content_type", function(type) {
    if (type) {
        this._obj.headers["content-type"].should.eql(type);
    } else {
        this._obj.headers["content-type"].should.be.a("string");
    }
});

chai.Assertion.addMethod("yt_error_body", function() {
    var body;
    try {
        body = JSON.parse(this._obj);
        body.should.have.property("code");
        body.code.should.be.a("number");
        body.should.have.property("message");
        body.message.should.be.a("string");
        body.should.have.property("attributes");
        body.attributes.should.be.an("object");
        body.should.have.property("inner_errors");
        body.inner_errors.should.be.an("object");
        body.inner_errors.should.be.instanceof(Array);
    } catch (ex) {
        assert.fail(
            "this._obj.body is not a well-formed JSON",
            "this._obj.body is a well-formed JSON",
            ex.toString());
    }
});

chai.Assertion.addMethod("yt_error", function() {
    var rsp = this._obj;
    rsp.should.be.http4xx;
    rsp.should.have.content_type("application/json");
    rsp.body.should.be.yt_error;
});

