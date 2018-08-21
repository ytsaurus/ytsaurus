var querystring = require("querystring");
var Q = require("bluebird");

var YtApplicationUpravlyator = require("../lib/middleware/application_upravlyator").that;
var YtError = require("../lib/error").that;
var YtRegistry = require("../lib/registry").that;

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;
var die = require("./common_http").die;

function stubServer(done)
{
    return srv(YtApplicationUpravlyator(), done);
}

function stubRegistry()
{
    var config = { upravlyator: {} };
    var logger = stubLogger();

    YtRegistry.set("config", config);
    YtRegistry.set("logger", logger);
    YtRegistry.set("driver", { executeSimple: function(){ return Q.resolve(); } });
    YtRegistry.set("authority", { ensureUser: function(){ return Q.resolve(); } });
    YtRegistry.set("robot_yt_idm", "root");
}

function map(object, iterator, context)
{
    var results = [];
    for (var property in object) {
        if (object.hasOwnProperty(property)) {
            results[results.length] = iterator.call(
                context,
                property,
                object[property]);
        }
    }
    return results;
}

var FIXTURE_USERS = {
    "sandello": {
        name: "sandello",
        member_of: [ "unmanaged1", "managed1" ],
        upravlyator_managed: "true",
    },
    "stunder": {
        name: "stunder",
        member_of: [ "unmanaged1" ],
        upravlyator_managed: "true",
    },
    "anonymous": {
        name: "anonymous",
        member_of: [ "unmanaged2", "managed2" ],
    },
};

var FIXTURE_GROUPS = {
    "unmanaged1": {
        name: "unmanaged1",
    },
    "unmanaged2": {
        name: "unmanaged2",
    },
    "unmanaged3": {
        name: "unmanaged3",
        upravlyator_managed: "false",
    },
    "managed1": {
        name: "managed1",
        upravlyator_managed: "true",
    },
    "managed2": {
        name: "managed2",
        upravlyator_managed: "true",
        upravlyator_name: "islay",
    },
    "managed3": {
        name: "managed3",
        upravlyator_managed: "true",
        upravlyator_name: "speyside",
        upravlyator_help: "delightful whiskey with a fruity taste",
    },
    "managed4": {
        name: "managed4",
        upravlyator_managed: "true",
        upravlyator_responsibles: ["pupkin", "vasia"],
    },
};

function mockGetUser(mock, name)
{
    var result;
    if (typeof(FIXTURE_USERS[name]) === "undefined") {
        result = Q.reject(new YtError("Not Found").withCode(500));
    } else {
        result = Q.resolve(FIXTURE_USERS[name]);
    }
    return mock
        .expects("executeSimple")
        .once()
        .withExactArgs("get", sinon.match({
            path: "//sys/users/" + name + "/@"
        }))
        .returns(result);
}

function mockGetGroup(mock, name)
{
    var result;
    if (typeof(FIXTURE_GROUPS[name]) === "undefined") {
        result = Q.reject(new YtError("Not Found").withCode(500));
    } else {
        result = Q.resolve(FIXTURE_GROUPS[name]);
    }
    return mock
        .expects("executeSimple")
        .once()
        .withExactArgs("get", sinon.match({
            path: "//sys/groups/" + name + "/@"
        }))
        .returns(result);
}

function mockListUsers(mock)
{
    var result = Q.resolve(map(FIXTURE_USERS, function(value, attributes) {
        return { $value: value, $attributes: attributes };
    }));
    return mock
        .expects("executeSimple")
        .once()
        .withExactArgs("list", sinon.match({
            path: "//sys/users",
            attributes: [
                "upravlyator_managed",
                "member_of",
            ],
        }))
        .returns(result);
}

function mockListGroups(mock)
{
    var result = Q.resolve(map(FIXTURE_GROUPS, function(value, attributes) {
        return { $value: value, $attributes: attributes };
    }));
    return mock
        .expects("executeSimple")
        .once()
        .withExactArgs("list", sinon.match({
            path: "//sys/groups",
            attributes: [
                "upravlyator_managed",
                "upravlyator_name",
                "upravlyator_help",
                "upravlyator_responsibles",
            ],
        }))
        .returns(result);
}

function mockMetaStateFailure(mock)
{
    return mock
        .expects("executeSimple")
        .returns(Q.reject(new YtError("Something is broken")));
}

function mockUserExists(mock, login, exists)
{
    return mock
        .expects("ensureUser")
        .withExactArgs(sinon.match.any, login, true)
        .returns(exists ? Q.resolve() : Q.reject(new YtError("Something is broken")));
}

////////////////////////////////////////////////////////////////////////////////

describe("ApplicationUpravlyator", function() {
    beforeEach(function(done) {
        stubRegistry();
        this.server = stubServer(done);
    });

    afterEach(function(done) {
        die(this.server, done);
        this.server = null;
        YtRegistry.clear();
    });

    describe("/info", function() {
        it("should report only managed groups", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockListGroups(mock);
            ask("GET", "/info", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json.roles).to.be.an("object");
                expect(rsp.json.roles.slug).to.eql("group");
                expect(rsp.json.roles.values).to.eql({
                    managed1: { name: "managed1" },
                    managed2: { name: "islay" },
                    managed3: { name: "speyside", help: "delightful whiskey with a fruity taste" },
                    managed4: {
                        name: "managed4",
                        responsibilities: [
                            { username: "pupkin", notify: false },
                            { username: "vasia", notify: false },
                        ]
                    },
                });
            }, done).end();
        });

        it("should fail when metastate is not available", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockMetaStateFailure(mock).once();
            ask("GET", "/info", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.error).to.be.a("string");
            }, done).end();
        });
    });

    describe("/add-role", function() {
        it("should pass on existing role", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "managed1");
            var mock2 = sinon.mock(YtRegistry.get("authority"));
            mockUserExists(mock2, "sandello", true);
            ask("POST", "/add-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                mock2.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json.warning).to.be.a("string");
                rsp.json.warning.should.match(/\bsandello\b/);
                rsp.json.warning.should.match(/\bmanaged1\b/);
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "managed1" })
            }));
        });

        it("should pass on unknown role", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "managed2");
            mock
                .expects("executeSimple").once()
                .withExactArgs("add_member", sinon.match({
                    member: "sandello",
                    group: "managed2"
                }));
            var mock2 = sinon.mock(YtRegistry.get("authority"));
            mockUserExists(mock2, "sandello", true);
            ask("POST", "/add-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                mock2.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json).to.have.keys([ "code" ]);
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "managed2" })
            }));
        });
    });

    describe("/remove-role", function() {
        it("should pass on existing role", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "managed1");
            mock
                .expects("executeSimple").once()
                .withExactArgs("remove_member", sinon.match({
                    member: "sandello",
                    group: "managed1"
                }));
            var mock2 = sinon.mock(YtRegistry.get("authority"));
            mockUserExists(mock2, "sandello", true);
            ask("POST", "/remove-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                mock2.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json).to.have.keys([ "code" ]);
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "managed1" })
            }));
        });

        it("should pass on unknown role", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "managed2");
            var mock2 = sinon.mock(YtRegistry.get("authority"));
            mockUserExists(mock2, "sandello", true);
            ask("POST", "/remove-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                mock2.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json.warning).to.be.a("string");
                rsp.json.warning.should.match(/\bsandello\b/);
                rsp.json.warning.should.match(/\bmanaged2\b/);
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "managed2" })
            }));
        });
    });

    [ "/add-role", "/remove-role" ].forEach(function(method) {
    describe(method, function() {
        it("should prohibit modifying unmanaged users", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "anonymous");
            mockGetGroup(mock, "managed3");
            ask("POST", method, {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.fatal).to.be.a("string");
                expect(rsp.json.fatal).to.match(/is not managed/);
            }, done).end(querystring.stringify({
                login: "anonymous",
                role: JSON.stringify({ group: "managed3" })
            }));
        });

        it("should prohibit modifying unmanaged groups", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "unmanaged2");
            ask("POST", method, {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.fatal).to.be.a("string");
                expect(rsp.json.fatal).to.match(/is not managed/);
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "unmanaged2" })
            }));
        });

        it("should fail on unknown user", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "unknown");
            mockGetGroup(mock, "managed3");
            ask("POST", method, {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.fatal).to.be.a("string");
                expect(rsp.json.fatal).to.match(/no such user/i);
            }, done).end(querystring.stringify({
                login: "unknown",
                role: JSON.stringify({ group: "managed3" })
            }));
        });

        it("should fail when metastate is not available", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockMetaStateFailure(mock).twice();
            ask("POST", method, {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.error).to.be.a("string");
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "sandello" })
            }));
        });
    });
    });

    describe("/add-role", function() {
        it("should fail on unknown group", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "unknown");
            ask("POST", "/add-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.fatal).to.be.a("string");
                expect(rsp.json.fatal).to.match(/no such group/i);
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "unknown" })
            }));
        });
    });

    describe("/remove-role", function() {
        it("should pass on unknown group", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "unknown");
            ask("POST", "/remove-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.eql(0);
            }, done).end(querystring.stringify({
                login: "sandello",
                role: JSON.stringify({ group: "unknown" })
            }));
        });
    });

    describe("/get-user-roles", function() {
        it("should return empty list on unknown user", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "unknown");
            mockListGroups(mock);
            ask("GET", "/get-user-roles?login=unknown", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json.roles).to.be.an("array");
                expect(rsp.json.roles).to.be.empty;
            }, done).end();
        });

        it("should return empty list on unmanaged user", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "anonymous");
            mockListGroups(mock);
            ask("GET", "/get-user-roles?login=anonymous", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json.roles).to.be.an("array");
                expect(rsp.json.roles).to.be.empty;
            }, done).end();
        });

        it("should properly show/hide (un)managed groups for managed users", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockListGroups(mock);
            ask("GET", "/get-user-roles?login=sandello", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json.roles).to.be.an("array");
                expect(rsp.json.roles).to.eql([ { group: "managed1" } ]);
            }, done).end();
        });

        it("should fail when metastate is not available", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockMetaStateFailure(mock).twice();
            ask("GET", "/get-user-roles?login=sandello", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.error).to.be.a("string");
            }, done).end();
        });
    });

    describe("/get-all-roles", function() {
        it("should properly show/hide (un)managed users/groups", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockListUsers(mock);
            mockListGroups(mock);
            ask("GET", "/get-all-roles", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.eql(0);
                expect(rsp.json.users).to.be.an("array");
                expect(rsp.json.users).to.eql([
                    { login: "sandello", roles: [ { group: "managed1" } ] }
                ]);
            }, done).end();
        });

        it("should fail when metastate is not available", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockMetaStateFailure(mock).twice();
            ask("GET", "/get-all-roles", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
                expect(rsp.json.code).to.not.eql(0);
                expect(rsp.json.error).to.be.a("string");
            }, done).end();
        });
    });
});
