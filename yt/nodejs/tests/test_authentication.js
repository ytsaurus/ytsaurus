var YtRegistry = require("../lib/registry").that;
var YtAuthority = require("../lib/authority").that;
var YtAuthentication = require("../lib/middleware/authentication").that;

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;

var nock = require("nock");

function stubServer(done)
{
    return srv(
        YtAuthentication(),
        function(req, rsp, next) {
            rsp.end(req.authenticated_user);
        },
        done);
}

function stubRegistry()
{
    var config = {
        services: {
            blackbox: {
                host: "localhost",
                port: 9000,
                nodelay: true,
                timeout: 100,
                retries: 2
            }
        },
        authentication: {
            enable: true,
            grant: "ytgrant",
            guest_login: "ytguest",
            guest_realm: "ytguest",
            cache_max_size: 3000,
            cache_max_age: 60 * 1000,
            realms: [
                {
                    key: "local",
                    type: "local",
                    tokens: {
                        "local-token": "darth-vader",
                    },
                },
                {
                    key: "ytrealm-key",
                    type: "oauth",
                    client_id: "ytrealm-id",
                    client_secret: "ytrealm-secret"
                },
            ],
        },
    };

    var logger = stubLogger();

    YtRegistry.set("config", config);
    YtRegistry.set("logger", logger);
    YtRegistry.set("authority", new YtAuthority(config.authentication));
}

////////////////////////////////////////////////////////////////////////////////

describe("YtAuthentication", function() {
    beforeEach(function(done) {
        stubRegistry();
        this.server = stubServer(done);
    });

    afterEach(function(done) {
        this.server.close(done);
        this.server = null;
        YtRegistry.clear();
    });

    it("should authenticate as guest without Authorization header", function(done) {
        ask("GET", "/", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.eql("ytguest");
        }, done).end();
    });

    it("should reject invalid Authorization headers, 1", function(done) {
        ask("GET", "/",
        { "Authorization": "i-am-a-cool-hacker" },
        function(rsp) { rsp.statusCode.should.eql(401); }, done).end();
    });

    it("should reject invalid Authorization headers, 2", function(done) {
        ask("GET", "/",
        { "Authorization": "OAuth" },
        function(rsp) { rsp.statusCode.should.eql(401); }, done).end();
    });

    it("should accept valid Blackbox tokens", function(done) {
        var mock = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=remote-token")
            .reply(200, {
                error: "OK",
                login: "anakin",
                oauth: { client_id: "ytrealm-id", scope: "ytgrant" }
            });
        ask("GET", "/",
        { "Authorization": "OAuth remote-token" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.eql("anakin");
            mock.done();
        }, done).end();
    });

    it("should accept valid local tokens", function(done) {
        ask("GET", "/",
        { "Authorization": "OAuth local-token" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.eql("darth-vader");
        }, done).end();
    });

    it("should reject invalid tokens", function(done) {
        var mock = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=invalid-token")
            .reply(200, { error: "ANY_ERROR" });
        ask("GET", "/",
        { "Authorization": "OAuth invalid-token" },
        function(rsp) {
            rsp.statusCode.should.eql(401);
            mock.done();
        }, done).end();
    });

    [ 400, 500 ].forEach(function(replyCode) {
        it("should fail on " + replyCode + " Blackbox reply", function(done) {
            var mock = nock("http://localhost:9000")
                .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=bad-http-reply")
                .reply(replyCode, { error: ":7" })
                .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=bad-http-reply")
                .reply(replyCode, { error: ":7" });
            ask("GET", "/",
            { "Authorization": "OAuth bad-http-reply" },
            function(rsp) {
                rsp.statusCode.should.eql(503);
                mock.done();
            }, done).end();
        });
    });

    it("should fail on Blackbox soft failure", function(done) {
        var mock = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=soft-failure")
            .reply(200, { exception: "TRY_AGAIN" })
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=soft-failure")
            .reply(200, { exception: "TRY_AGAIN" })
        ask("GET", "/",
        { "Authorization": "OAuth soft-failure" },
        function(rsp) {
            rsp.statusCode.should.eql(503);
            mock.done();
        }, done).end();
    });

    it("should reject invalid token issuer id", function(done) {
        var mock = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=obi-wan-kenobi")
            .reply(200, {
                error: "OK",
                login: "obi-wan",
                oauth: { client_id: "jedi", scope: "ytgrant" }
            });
        ask("GET", "/",
        { "Authorization": "OAuth obi-wan-kenobi" },
        function(rsp) {
            rsp.statusCode.should.eql(401);
            mock.done();
        }, done).end();
    });

    it("should reject invalid token grants", function(done) {
        var mock = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=qui-gon-jinn")
            .reply(200, {
                error: "OK",
                login: "qui-gon",
                oauth: { client_id: "ytrealm-id", scope: "force" }
            });
        ask("GET", "/",
        { "Authorization": "OAuth qui-gon-jinn" },
        function(rsp) {
            rsp.statusCode.should.eql(401);
            mock.done();
        }, done).end();
    });

    it("should retry failed requests", function(done) {
        var mock = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=retryable-token")
            .reply(500)
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=retryable-token")
            .reply(200, {
                error: "OK",
                login: "amidala",
                oauth: { client_id: "ytrealm-id", scope: "ytgrant" }
            });
        ask("GET", "/",
        { "Authorization": "OAuth retryable-token" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.eql("amidala");
            mock.done();
        }, done).end();
    });
});
