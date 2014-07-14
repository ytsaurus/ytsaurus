var qs = require("querystring");

var YtApplicationAuth = require("../lib/middleware/application_auth").that;
var YtError = require("../lib/error").that;
var YtRegistry = require("../lib/registry").that;
var YtAuthority = require("../lib/authority").that;

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;
var die = require("./common_http").die;

var nock = require("nock");

function stubServer(done)
{
    return srv(YtApplicationAuth(), done);
}

function stubRegistry()
{
    var config = {
        services : {
            oauth : {
                host: "localhost",
                port: 8000,
                nodelay: true,
                timeout: 1000,
                retries: 2
            },
            blackbox : {
                host: "localhost",
                port: 9000,
                nodelay: true,
                timeout: 1000,
                retries: 2
            },
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
                {
                    key: "first",
                    client_id: "first_client_id",
                    client_secret: "first_client_secret"
                },
                {
                    key: "second",
                    client_id: "second_client_id",
                    client_secret: "second_client_secret"
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

describe("ApplicationAuth", function() {
    beforeEach(function(done) {
        stubRegistry();
        this.server = stubServer(done);
    });

    afterEach(function(done) {
        die(this.server, done);
        this.server = null;
        YtRegistry.clear();
    });

    [ "first", "second" ].forEach(function(realm) {
    it("should redirect with state to the proper OAuth provider for realm '" + realm + "'",
    function(done) {
        ask("GET", "/new?foo=bar&spam=ham&realm=" + realm, {}, function(rsp) {
            rsp.should.be.http3xx;
            rsp.headers["location"].should.eql(
                "http://localhost/authorize" +
                "?response_type=code" +
                "&display=popup" +
                "&client_id=" + realm + "_client_id" +
                "&state=" + qs.escape('{' +
                    '"foo":"bar",' +
                    '"spam":"ham",' +
                    '"realm":"' + realm + '"' +
                '}'));
        }, done).end();
    });
    });

    it("should request token and display it to the user", function(done) {
        var params = qs.stringify({
            code: 123456789,
            state: JSON.stringify({
                realm: "ytrealm-key",
            })
        });
        var mock1 = nock("http://localhost:8000")
            .post("/token", "code=123456789&grant_type=authorization_code&client_id=ytrealm-id&client_secret=ytrealm-secret")
            .reply(200, {
                access_token: "deadbabedeadbabe"
            });
        var mock2 = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=deadbabedeadbabe")
            .reply(200, {
                error: "OK",
                login: "scorpion",
                oauth: { client_id: "ytrealm-id", scope: "ytgrant" }
            });
        ask("GET", "/new?" + params, {}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.match(/\bdeadbabedeadbabe\b/);
            mock1.done();
            mock2.done();
        }, done).end();
    });

    it("should request token and redirect the user", function(done) {
        var params = qs.stringify({
            code: 123456789,
            state: JSON.stringify({
                realm: "ytrealm-key",
                return_path: "http://ya.ru/?my_foo=a&my_bar=b"
            })
        });
        var mock1 = nock("http://localhost:8000")
            .post("/token", "code=123456789&grant_type=authorization_code&client_id=ytrealm-id&client_secret=ytrealm-secret")
            .reply(200, {
                access_token: "deadbabedeadbabe"
            });
        var mock2 = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=deadbabedeadbabe")
            .reply(200, {
                error: "OK",
                login: "scorpion",
                oauth: { client_id: "ytrealm-id", scope: "ytgrant" }
            });
        ask("GET", "/new?" + params, {}, function(rsp) {
            rsp.should.be.http3xx;
            rsp.headers["location"].should.eql("http://ya.ru/?my_foo=a&my_bar=b&token=deadbabedeadbabe&login=scorpion");
            mock1.done();
            mock2.done();
        }, done).end();
    });

    it("should respond with an error on GET /login request", function(done) {
        ask("GET", "/login", {}, function(rsp) {
            rsp.should.be.http4xx;
        }, done).end();
    });

    it("should respond with an error on empty POST /login request", function(done) {
        ask("POST", "/login", {}, function(rsp) {
            rsp.should.be.http4xx;
        }, done).end();
    });

    it("should respond with an login & realm on proper POST /login request", function(done) {
        var mock = nock("http://localhost:9000")
            .get("/blackbox?method=oauth&format=json&userip=127.0.0.1&oauth_token=foobar")
            .reply(200, {
                error: "OK",
                login: "donkey",
                oauth: { client_id: "ytrealm-id", scope: "ytgrant" }
            });
        ask("POST", "/login", {}, function(rsp) {
            rsp.should.be.http2xx;
            expect(rsp.json.login).to.eql("donkey");
            expect(rsp.json.realm).to.eql("blackbox-ytrealm-key");
            mock.done();
        }, done).end(qs.stringify({
            token: "foobar"
        }));
    });


});
