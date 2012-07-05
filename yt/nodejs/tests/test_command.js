var http = require("http");
var connect = require("connect");

var YtDriver = require("../lib/driver").that;
var YtCommand = require("../lib/command").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YTTEST/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("\nYT Tests:", x); };
} else {
    __DBG = function(){};
}

var __HTTP_PORT = 40000 + parseInt(Math.random() * 10000);
var __HTTP_HOST = "127.0.0.1";

// A bunch of helpful assertions to use while testing HTTP.

chai.Assertion.addProperty('http2xx', function() {
    this._obj.statusCode.should.be.within(200, 300);
});

chai.Assertion.addProperty('http3xx', function() {
    this._obj.statusCode.should.be.within(300, 400);
});

chai.Assertion.addProperty('http4xx', function() {
    this._obj.statusCode.should.be.within(400, 500);
});

chai.Assertion.addProperty('http5xx', function() {
    this._obj.statusCode.should.be.within(500, 600);
});

chai.Assertion.addMethod('content_type', function(mime) {
    this._obj.headers["content-type"].should.eql(mime);
});

////////////////////////////////////////////////////////////////////////////////

// This will spawn a (mock of a) real API server.
function spawnServer(driver, watcher) {
    var sink = function(){};
    var logger = { };

    [ "info", "warn", "debug", "error" ].forEach(function(level) {
        logger[level] = sink;
    });

    return connect()
        .use("/api", function(req, rsp) {
            return (new YtCommand(
                logger, driver, watcher, req, rsp
            )).dispatch();
        })
        .listen(__HTTP_PORT, __HTTP_HOST);
}

// This stub provides a real driver instance which simply pipes all data through.
function stubDriver(echo, low_watermark, high_watermark) {
    var config = {
        "low_watermark" : 100,
        "high_watermark" : 200,
        "driver" : {
            "masters" : { "addresses" : [ "localhost:0" ] },
            "logging" : { "rules" : [], "writers" : {} }
        }
    };

    return new YtDriver(echo, config);
}

// This stub provides a constant watcher which is either choking or not.
function stubWatcher(is_choking) {
    return { tackle: function(){}, is_choking: function() { return is_choking; } };
}

// This is a helper method to produce HTTP requests.
// NB: Do not forget to call .end() on a returned object since it is a stream.
function ask(method, path, additional_options, done, callback) {
    var options = connect.utils.merge({
        method : method,
        path : path,
        port : __HTTP_PORT,
        host : __HTTP_HOST
    }, additional_options);

    var request = http.request(options, function(rsp) {
        var response = "";

        rsp.on("data", function(chunk) { response += chunk.toString(); });
        rsp.on("end", function() {
            try {
                if (response.length > 0) {
                    try {
                        var responseParsed = JSON.parse(response);

                        if (responseParsed.hasOwnProperty("error")) {
                            __DBG("*** HTTP Response Error:\n" + responseParsed.error);
                            delete responseParsed.error;
                        }
                        if (responseParsed.hasOwnProperty("error_trace")) {
                            __DBG("*** HTTP Response Error Stack:\n" + responseParsed.error_trace);
                            delete responseParsed.error_trace;
                        }

                        var responseFormatted = JSON.stringify(responseParsed, null, 2);
                        __DBG("*** HTTP Response Body: " + responseFormatted);
                    } catch(err) {
                        __DBG("*** HTTP Response Body: " + response);
                    }
                }
                
                rsp.body = response;
                callback.call(this, rsp, done);
                done();
            } catch(err) {
                done(err);
            }
        });
    });

    return request;
}

////////////////////////////////////////////////////////////////////////////////

describe("Yt - http method selection", function() {
    before(function() {
        this.server = spawnServer(stubDriver(true), stubWatcher(false));
    });

    after(function() {
        this.server.close();
        this.server = null;
    });

    [ "/api/get", "/api/download", "/api/read" ]
    .forEach(function(entry_point) {
        it("should use GET for " + entry_point, function(done) {
            ask("GET", entry_point, {}, done, function(rsp) {
                rsp.should.be.http2xx;
            }).end();
        });
    });
    
    [ "/api/set", "/api/upload", "/api/write" ]
    .forEach(function(entry_point) {
        it("should use PUT for " + entry_point, function(done) {
            ask("PUT", entry_point, {}, done, function(rsp) {
                rsp.should.be.http2xx;
            }).end();
        });
    });

    [ "/api/map", "/api/reduce", "/api/sort" ]
    .forEach(function(entry_point) {
        it("should use POST for " + entry_point, function(done) {
            ask("POST", entry_point, {}, done, function(rsp) {
                rsp.should.be.http2xx;
            }).end();
        });
    });
});

describe("Yt - command name", function() {
    before(function() {
        this.server = spawnServer(stubDriver(true), stubWatcher(false));
    });

    after(function() {
        this.server.close();
        this.server = null;
    });

    it("should allow good names", function(done) {
        ask("GET", "/api/get", {}, done, function(rsp) {
            rsp.should.be.http2xx;
        }).end();
    });

    it("should disallow bad names", function(done) {
        ask("GET", "/api/$$$", {}, done, function(rsp) {
            rsp.statusCode.should.eql(400);
            rsp.should.have.content_type("application/json");
        }).end();
    });

    it("should return 404 when the name is unknown", function(done) {
        ask("GET", "/api/unknown_but_valid_name", {}, done, function(rsp) {
            rsp.statusCode.should.eql(404);
            rsp.should.have.content_type("application/json");
        }).end();
    });

    it("should display a reference when the name is empty", function(done) {
        ask("GET", "/api", {}, done, function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/json");

            var body = JSON.parse(rsp.body);
            body.should.be.instanceof(Array);

            body.forEach(function(item) {
                item.should.have.property("name");
                item.should.have.property("input_type");
                item.should.have.property("output_type");
                item.should.have.property("is_volatile");
                item.should.have.property("is_heavy");
            });
        }).end();
    });
});

describe("Yt - command heaviness", function() {
    var driver = stubDriver(true);

    [ "download", "upload", "read", "write" ]
    .forEach(function(name) {
        it("should affect '" + name + "'", function() {
            driver.find_command_descriptor(name).is_heavy.should.be.true;
        });
    });

    describe("when there is no workload", function() {
        before(function() {
            this.server = spawnServer(driver, stubWatcher(false));
        });

        after(function() {
            this.server.close();
            this.server = null;
        });

        it("should allow light commands ", function(done) {
            ask("GET", "/api/get", {}, done, function(rsp) {
                rsp.should.be.http2xx;
            }).end();
        });

        it("should allow heavy commands ", function(done) {
            ask("GET", "/api/read", {}, done, function(rsp) {
                rsp.should.be.http2xx;
            }).end();
        });
    });

    describe("when there is workload", function() {
        before(function() {
            this.server = spawnServer(driver, stubWatcher(true));
        });

        after(function() {
            this.server.close();
            this.server = null;
        });

        it("should allow light commands ", function(done) {
            ask("GET", "/api/get", {}, done, function(rsp) {
                rsp.should.be.http2xx;
            }).end();
        });

        it("should disallow heavy commands ", function(done) {
            ask("GET", "/api/read", {}, done, function(rsp) {
                rsp.statusCode.should.eql(503);
            }).end();
        });
    });
});

describe("Yt - command parameters", function() {
    beforeEach(function() {
        this.driver = stubDriver(true);
        this.server = spawnServer(this.driver,  stubWatcher(false));
    });

    afterEach(function() {
        this.driver = null;

        this.server.close();
        this.server = null;
    });

    it("should set no defaults", function(done) {
        var self = this;
        self.stub = sinon.spy(self.driver, "execute");

        ask("GET", "/api/get", {}, done, function(rsp) {
            rsp.should.be.http2xx;
            self.stub.should.have.been.calledOnce;
            self.stub.firstCall.args[0].should.eql("get");
            self.stub.firstCall.args[7].should.eql({});
        }).end();
    });

    it("should take query string parameters", function(done) {
        var self = this;
        self.stub = sinon.spy(self.driver, "execute");

        ask("GET", "/api/get?who=me&path=/&foo=bar", {}, done, function(rsp) {
            rsp.should.be.http2xx;
            self.stub.should.have.been.calledOnce;
            self.stub.firstCall.args[0].should.eql("get");
            self.stub.firstCall.args[7].should.eql({
                "who" : "me", "path" : "/", "foo" : "bar"
            });
        }).end();
    });

    it("should take body parameters for POST methods");

    it("should not take body parameters for PUT methods");

    it("should prefer body over query string");

    // check basic conversions
    // check attributes
    // check binary encoding
    it("should properly translate into a YSON");
});

describe("Yt - input format selection", function() {
    it("should use application/json as a default for structured data");

    it("should use application/octet-stream as a default for binary data");

    it("should use text/tab-separated-values as a default for tabular data");

    it("should respect content-type header if it specifies known format");

    it("should fail if content-type header specifies unknown format");

    it("should respect custom header and discard mime-type accordingly");
});

describe("Yt - output format selection", function() {
    it("should use application/json as a default for structured data");

    it("should use application/octet-stream as a default for binary data");

    it("should use text/tab-separated-values as a default for tabular data");

    it("should respect accept header if it specifies known format");

    it("should fail if accept header specifies unknown format");

    it("should respect custom header and discard mime-type accordingly");
});

describe("Yt - input compression", function() {
    it("should support gzip");

    it("should support deflate");

    it("should support lzop");

    it("should fail on unknown encoding");
});

describe("Yt - output compression", function() {
    it("should support gzip");

    it("should support deflate");

    it("should support lzop");

    it("should fail on unknown encoding");
});

describe("Yt - permissions - w/ .path", function() {
    it("should allow mutable operations on //tmp");

    it("should allow mutable operations on //home");

    it("should disallow mutable operations on //sys");
});

// TODO: Check also:
// - spec.input_table_paths
// - spec.input_table_path
// - spec.output_table_paths
// - spec.output_table_path

describe("Yt - error handling", function() {
    it("should set trailer headers on failure");
});
