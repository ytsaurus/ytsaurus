var expect = require("chai").expect;
var assert = require("chai").assert;

var http = require("http");
var connect = require("connect");
var app = require("./srv");

////////////////////////////////////////////////////////////////////////////////

describe("http application", function() {
    var APP_HTTP_PORT = 8000;
    var APP_HTTP_HOST = "127.0.0.1";

    function app_request(method, path, additional_options, callback) {
        var options = connect.utils.merge({
            method: method, path: path,
            port: APP_HTTP_PORT, host: APP_HTTP_HOST
        }, additional_options);
        return http.request(options, callback);
    }

    before(function() {
        this.server = connect().use(app.YtApplication());
        this.server.listen(APP_HTTP_PORT, APP_HTTP_HOST);
    });

    after(function() {
        this.server = null;
    });

    describe("should determine input format", function() {
        it("with specified Accept", function(done) {
            app_request("POST", "/set?path=//tmp/test",
                { headers : { "Content-Type" : "application/json" } },
                function(rsp) {
                    expect(rsp.headers["x-yt-input-format"]).to.include("json");
                    done();
                }
            ).end("{}");
        });

        it("with specified X-YT-Input-Format", function(done) {
            app_request("POST", "/set?path=//tmp/test",
                { headers : { "X-YT-Input-Format" : "json" } },
                function(rsp) {
                    expect(rsp.headers["x-yt-input-format"]).to.include("json");
                    done();
                }
            ).end("{}");
        });
    });

    describe("should determine output format", function() {
        it("with specified Accept", function(done) {
            app_request("GET", "/get?path=/",
                { headers : { "Accept" : "application/json" } },
                function(rsp) {
                    expect(rsp.headers["content-type"]).to.include("application/json");
                    expect(rsp.headers["x-yt-output-format"]).to.include("json");
                    done();
                }
            ).end();
        });

        it("with specified X-YT-Output-Format", function(done) {
            app_request("GET", "/get?path=/",
                { headers : { "X-YT-Output-Format" : "json" } },
                function(rsp) {
                    expect(rsp.headers["content-type"]).to.include("application/json");
                    expect(rsp.headers["x-yt-output-format"]).to.include("json");
                    done();
                }
            ).end();
        });
    });

    describe("/get", function() {
        it("should require a path parameter", function(done) {
            app_request("GET", "/get", {}, function(rsp) {
                expect(rsp.statusCode).to.be.equal(400);
                done();
            }).end();
            app_request("GET", "/get?path=/", {}, function(rsp) {
                expect(rsp.statusCode).to.be.equal(200);
                done();
            }).end();
        });

        it("should pass with GET method", function(done) {
            app_request("GET", "/get?path=/", {}, function(rsp) {
                expect(rsp.statusCode).to.be.equal(200);
                done();
            }).end();
        });

        it("should fail with POST method", function(done) {
            app_request("POST", "/get?path=/", {}, function(rsp) {
                expect(rsp.statusCode).to.be.equal(400);
                done();
            }).end();
        });
        
        it("should fail with PUT method", function(done) {
            app_request("PUT", "/get?path=/", {}, function(rsp) {
                expect(rsp.statusCode).to.be.equal(400);
                done();
            }).end();
        });

        it("should return meaningful result", function(done) {
            assert.fail();
        });
    });
});
