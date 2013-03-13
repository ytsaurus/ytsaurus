var querystring = require("querystring");
var Q = require("q");

var YtApplicationUpravlyator = require("../lib/middleware/application_upravlyator").that;
var YtError = require("../lib/error").that;
var YtRegistry = require("../lib/registry").that;

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;

function stubServer(done)
{
    return srv(
        YtApplicationUpravlyator(),
        done);
}

function stubRegistry()
{
    var config = {
        upravlyator: {
            interval: 15000,
            cache: false
        },
    };

    var logger = stubLogger();

    YtRegistry.set("config", config);
    YtRegistry.set("logger", logger);
    YtRegistry.set("driver", { executeSimple: function(){} });
}

var FIXTURE_LIST_GROUPS = [
    "unmanaged1",
    {
        $value: "unmanaged2",
        $attributes: {}
    },
    {
        $value: "unmanaged3",
        $attributes: { upravlyator_managed: "false" }
    },
    {
        $value: "managed1",
        $attributes: { upravlyator_managed: "true" }
    },
    {
        $value: "managed2",
        $attributes: {
            upravlyator_managed: "true",
            upravlyator_name: "islay",
        }
    },
    {
        $value: "managed3",
        $attributes: {
            upravlyator_managed: "true",
            upravlyator_name: "speyside",
            upravlyator_help: "delightful whiskey with a fruity taste",
        }
    },
];

var FIXTURE_GET_USERS = {
    "sandello": {
        name: "sandello",
        member_of: [ "unmanaged1", "managed1" ],
        upravlyator_managed: "true"
    },
    "anonymous": {
        name: "anonymous",
        member_of: [ "unmanaged2", "managed2" ]
    },
    "unknown": Q.reject(new YtError("Not Found").withCode(500))
};

var FIXTURE_GET_GROUPS = {
    "unmanaged1": { name: "unmanaged1" },
    "unmanaged2": { name: "unmanaged2" },
    "unmanaged3": { name: "unmanaged3", upravlyator_managed: "false" },
    "managed1": { name: "managed1", upravlyator_managed: "true" },
    "managed2": { name: "managed2", upravlyator_managed: "true" },
    "managed3": { name: "managed3", upravlyator_managed: "true" },
    "unknown": Q.reject(new YtError("Not Found").withCode(500))
};

function mockGetUser(mock, name)
{
    return mock
        .expects("executeSimple")
        .once()
        .withExactArgs("get", sinon.match({
            path: "//sys/users/" + name + "/@"
        }))
        .returns(Q.resolve(FIXTURE_GET_USERS[name]));
}

function mockGetGroup(mock, name)
{
    return mock
        .expects("executeSimple")
        .once()
        .withExactArgs("get", sinon.match({
            path: "//sys/groups/" + name + "/@"
        }))
        .returns(FIXTURE_GET_GROUPS[name]);
}

////////////////////////////////////////////////////////////////////////////////

describe("Upravlyator", function() {
    beforeEach(function(done) {
        stubRegistry();
        this.server = stubServer(done);
    });

    afterEach(function(done) {
        this.server.close(done);
        this.server = null;
        YtRegistry.clear();
    });

    describe("/info", function() {
        it("should report only managed groups", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mock
                .expects("executeSimple").once()
                .withExactArgs(
                    "list",
                    sinon.match({
                        path: "//sys/groups",
                        attributes: [
                            "upravlyator_managed",
                            "upravlyator_name",
                            "upravlyator_help"
                        ]
                    })
                )
                .returns(FIXTURE_LIST_GROUPS);
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
                });
            }, done).end();
        });

        it("should fail when metastate is not available", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mock
                .expects("executeSimple").once()
                .withExactArgs("list", sinon.match({}))
                .returns(Q.reject(new YtError("Something is broken")));
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
            ask("POST", "/add-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
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
            ask("POST", "/add-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
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
            ask("POST", "/remove-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
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
            ask("POST", "/remove-role", {}, function(rsp) {
                rsp.should.be.http2xx;
                mock.verify();
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

        it("should fail on unknown group", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mockGetUser(mock, "sandello");
            mockGetGroup(mock, "unknown");
            ask("POST", method, {}, function(rsp) {
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

        it("should fail when metastate is not available", function(done) {
            var mock = sinon.mock(YtRegistry.get("driver"));
            mock
                .expects("executeSimple").twice()
                .withArgs("get")
                .returns(Q.reject(new YtError("Something is broken.")));
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

    describe("/get-user-roles", function() {
        it("should fail on unknown user");
        it("should properly show/hide (un)managed groups");
        it("should fail when metastate is not available");
    });

    describe("/get-all-roles", function() {
        it("should properly show/hide (un)managed users");
        it("should properly show/hide (un)managed groups");
        it("should fail when metastate is not available");
    });
});
