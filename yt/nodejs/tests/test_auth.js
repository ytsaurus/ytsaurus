var qs = require("querystring");

var YtApplicationAuth = require("../lib/middleware/application_auth").that;
var YtError = require("../lib/error").that;
var YtRegistry = require("../lib/registry").that;
var YtAuthority = require("../lib/authority").that;

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;

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
            grant: "ytgrant",
            guest_login: "ytguest",
            guest_realm: "ytguest",
            cache_max_size: 3000,
            cache_max_age: 60 * 1000,
            realms: [
                {
                    key: "ytrealm-key",
                    type: "oauth",
                    client_id: "ytrealm-id",
                    client_secret: "ytrealm-secret"
                },
                {
                    key: "first",
                    type: "oauth",
                    client_id: "first_client_id",
                    client_secret: "first_client_secret"
                },
                {
                    key: "second",
                    type: "oauth",
                    client_id: "second_client_id",
                    client_secret: "second_client_secret"
                },
            ],
        },
    };

    var logger = stubLogger();

    YtRegistry.set("config", config);
    YtRegistry.set("logger", logger);
    YtRegistry.set("driver", { executeSimple: function(){} });
    YtRegistry.set("authority", new YtAuthority(config.authentication));
}

////////////////////////////////////////////////////////////////////////////////

describe("XAuth", function() {
    beforeEach(function(done) {
        stubRegistry();
        this.server = stubServer(done);
    });

    afterEach(function(done) {
        this.server.close(done);
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
                "&state=%7B" +
                    "%22foo%22%3A%22bar%22%2C" +
                    "%22spam%22%3A%22ham%22%2C" +
                    "%22realm%22%3A%22" + realm + "%22" +
                "%7D");
        }, done).end();
    });
    });

    it("should request token and create non-existing user", function(done) {
        var params = qs.stringify({
            code: 123456789,
            state: JSON.stringify({
                realm: "ytrealm-key"
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
        var mock3 = sinon.mock(YtRegistry.get("driver"));
        mock3
            .expects("executeSimple").once()
            .withExactArgs("exists", sinon.match({
                path: "//sys/users/scorpion"
            }))
            .returns("false");
        mock3
            .expects("executeSimple").once()
            .withExactArgs("create", sinon.match({
                type: "user",
                attributes: { name: "scorpion" }
            }))
            .returns("0-0-0-0");
        ask("GET", "/new?" + params, {}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.match(/\bdeadbabedeadbabe\b/);
            mock1.done();
            mock2.done();
            mock3.verify();
        }, done).end();
    });
});
