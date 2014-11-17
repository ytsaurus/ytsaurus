var Q = require("q");

var YtAuthentication = require("../lib/middleware/authentication").that;
var YtAuthority = require("../lib/authority").that;
var YtError = require("../lib/error").that;
var YtRegistry = require("../lib/registry").that;

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;
var die = require("./common_http").die;

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
            cache_max_size: 3000,
            cache_max_token_age: 60 * 1000,
            cache_max_exist_age: 86400 * 1000,
            create_users_on_demand: true,
            guest_login: "ytguest",
            guest_realm: "ytguest",
            cypress: {
                enable: true,
                where: "//sys/tokens",
            },
            blackbox: {
                enable: true,
                grant: "ytgrant",
            },
            oauth: [
                {
                    key: "ytrealm-key",
                    client_id: "ytrealm-id",
                    client_secret: "ytrealm-secret"
                },
            ],
        },
    };

    var logger = stubLogger();
    var driver = { executeSimple: function(){} };

    YtRegistry.set("config", config);
    YtRegistry.set("logger", logger);
    YtRegistry.set("driver", driver);
    YtRegistry.set("authority", new YtAuthority(config.authentication, driver));
}

////////////////////////////////////////////////////////////////////////////////

describe("YtAuthentication", function() {
    beforeEach(function(done) {
        stubRegistry();
        this.server = stubServer(done);
    });

    afterEach(function(done) {
        die(this.server, done);
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

    it("should reject empty tokens", function(done) {
        ask("GET", "/",
        { "Authorization": "OAuth" },
        function(rsp) {
            rsp.statusCode.should.eql(401);
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

    it("should create user if he does not exist", function(done) {
        var mock1 = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=some-token")
            .reply(200, {
                error: "OK",
                login: "anakin",
                oauth: { client_id: "ytrealm-id", scope: "ytgrant" }
            });
        var mock2 = sinon.mock(YtRegistry.get("driver"))
        mock2
            .expects("executeSimple")
            .once()
            .withExactArgs("get", sinon.match({ path: "//sys/tokens/some-token" }))
            .returns(Q.reject(new YtError().withCode(500)));
        mock2
            .expects("executeSimple")
            .once()
            .withExactArgs("create", sinon.match({
                type: "user",
                attributes: { name: "anakin" }
            }))
            .returns(Q.resolve("0-0-0-0"));
        ask("GET", "/",
        { "Authorization": "OAuth some-token" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.eql("anakin");
            mock1.done();
            mock2.verify();
        }, done).end();
    });

    it("should not create user if he exists", function(done) {
        var mock1 = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=some-token")
            .reply(200, {
                error: "OK",
                login: "anakin",
                oauth: { client_id: "ytrealm-id", scope: "ytgrant" }
            });
        var mock2 = sinon.mock(YtRegistry.get("driver"))
        mock2
            .expects("executeSimple")
            .once()
            .withExactArgs("get", sinon.match({ path: "//sys/tokens/some-token" }))
            .returns(Q.reject(new YtError().withCode(500)));
        mock2
            .expects("executeSimple")
            .once()
            .withExactArgs("create", sinon.match({
                type: "user",
                attributes: { name: "anakin" }
            }))
            .returns(Q.reject(new YtError("Already exists").withCode(501)));
        ask("GET", "/",
        { "Authorization": "OAuth some-token" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.eql("anakin");
            mock1.done();
            mock2.verify();
        }, done).end();
    });

    it("should query Cypress at first", function(done) {
        var mock = sinon.mock(YtRegistry.get("driver"));
        mock
            .expects("executeSimple")
            .once()
            .withExactArgs("get", sinon.match({
                path: "//sys/tokens/unknown-token"
            }))
            .returns(Q.resolve("unknown-user"));
        mock
            .expects("executeSimple")
            .once()
            .withExactArgs("create", sinon.match({
                type: "user",
                attributes: { name: "unknown-user" }
            }))
            .returns(Q.reject(new YtError("Already exists").withCode(501)));
        ask("GET", "/",
        { "Authorization": "OAuth unknown-token" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.eql("unknown-user");
            mock.verify();
        }, done).end();
    });
});
