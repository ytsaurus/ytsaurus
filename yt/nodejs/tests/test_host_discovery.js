var http = require("http");
var connect = require("connect");

var YtHostDiscovery = require("../lib/host_discovery").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YTTEST/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("\nYT Tests:", x); };
} else {
    __DBG = function(){};
}

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
function spawnServer(neighbours, done) {
    var sink = function(){};
    var logger = { };

    [ "info", "warn", "debug", "error" ].forEach(function(level) {
        logger[level] = sink;
    });

    // Increment port to avoid EADDRINUSE failures.
    HTTP_PORT++;
    return connect()
        .use("/hosts", YtHostDiscovery(neighbours))
        .listen(HTTP_PORT, HTTP_HOST, done);
}

// This is a helper method to produce HTTP requests.
// NB: Do not forget to call .end() on a returned object since it is a stream.
function ask(method, path, additional_options, done, callback) {
    var options = connect.utils.merge({
        method : method,
        path : path,
        port : HTTP_PORT,
        host : HTTP_HOST
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

describe("host discovery", function() {
    before(function(done) {
        this.server = spawnServer([], done);
    });

    after(function(done) {
        this.server.close(done);
        this.server = null;
    });

    it("should produce randomized result");
    it("should be able to return application/json");
    it("should be able to return text/plain");
    it("should yell on a wrong content type");
});

