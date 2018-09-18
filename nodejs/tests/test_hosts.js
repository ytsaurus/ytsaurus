var Q = require("bluebird");

var YtApplicationHosts = require("../lib/middleware/application_hosts").that;
var YtRegistry = require("../lib/registry").that;

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;
var die = require("./common_http").die;

function stubServer(done)
{
    return srv(YtApplicationHosts(), done);
}

////////////////////////////////////////////////////////////////////////////////

describe("ApplicationHosts", function() {
    beforeEach(function(done) {
        this.coordinator = {getProxies: function(){}, allocateProxy: function(){}};
        YtRegistry.set("config", {
            show_ports: false,
            rewrite_yandex_team_domain: true,
            hosts: {
                fb: "HOST-fb.DOMAIN",
                fb867: "HOST-fb867.DOMAIN"
            }
        });
        YtRegistry.set("logger", stubLogger());
        YtRegistry.set("coordinator", this.coordinator);
        this.server = stubServer(done);
    });

    afterEach(function(done) {
        die(this.server, done);
        this.server = null;
        YtRegistry.clear();
    });

    function mockCoordinator(coordinator, role) {
        var mock = sinon.mock(coordinator);
        role = role || "data";
        mock
            .expects("getProxies")
            .once()
            .withExactArgs(role, false, false)
            .returns([
                {fitness: 500, name: "foo.yandex.net"},
                {fitness: 100, name: "bar.yandex.net"},
                {fitness: 300, name: "baz.yandex.net"},
                {fitness: 800, name: "abc.yandex.net"}]);
        mock
            .expects("allocateProxy")
            .once()
            .withExactArgs(role);
        return mock;
    }

    it("should return plain text list of proxies on /hosts call", function(done) {
        var mock = mockCoordinator(this.coordinator);
        ask("GET", "/", {"accept": "text/plain"}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.eql("bar.yandex.net\nbaz.yandex.net\nfoo.yandex.net\nabc.yandex.net");
            mock.verify();
        }, done).end();
    });

    it("should return json list of proxies on /hosts call", function(done) {
        var mock = mockCoordinator(this.coordinator);
        ask("GET", "/", {"accept": "application/json"}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.eql('["bar.yandex.net","baz.yandex.net","foo.yandex.net","abc.yandex.net"]');
            mock.verify();
        }, done).end();
    });

    it("should append suffix on /hosts/fb call", function(done) {
        var mock = mockCoordinator(this.coordinator);
        ask("GET", "/fb", {"accept": "text/plain"}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.eql("bar-fb.yandex.net\nbaz-fb.yandex.net\nfoo-fb.yandex.net\nabc-fb.yandex.net");
            mock.verify();
        }, done).end();
    });

    it("should fail on unknown suffix /hosts/xxx call", function(done) {
        var mock = mockCoordinator(this.coordinator);
        ask("GET", "/xxx", {"accept": "text/plain"}, function(rsp) {
            rsp.should.be.http4xx;
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should switch from yandex.net to yandex-team.ru on domain-originating call to /hosts", function(done) {
        var mock = mockCoordinator(this.coordinator);
        ask("GET", "/", {"accept": "text/plain", "origin": "https://yt.yandex-team.ru"}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.eql("bar.yandex-team.ru\nbaz.yandex-team.ru\nfoo.yandex-team.ru\nabc.yandex-team.ru");
            mock.verify();
        }, done).end();
    });

    it("should switch from yandex.net to yandex-team.ru for respective vhost call to /hosts", function(done) {
        var mock = mockCoordinator(this.coordinator);
        ask("GET", "/", {"accept": "text/plain", "host": "cluster.yt.yandex-team.ru"}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.eql("bar.yandex-team.ru\nbaz.yandex-team.ru\nfoo.yandex-team.ru\nabc.yandex-team.ru");
            mock.verify();
        }, done).end();
    });

    it("should enlist control proxies", function(done) {
        var mock = mockCoordinator(this.coordinator, "control");
        ask("GET", "/?role=control", {"accept": "text/plain"}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.eql("bar.yandex.net\nbaz.yandex.net\nfoo.yandex.net\nabc.yandex.net");
            mock.verify();
        }, done).end();
    });

    it("should enlist dyntables proxies", function(done) {
        var mock = mockCoordinator(this.coordinator, "dyntables");
        ask("GET", "/?role=dyntables", {"accept": "text/plain"}, function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.eql("bar.yandex.net\nbaz.yandex.net\nfoo.yandex.net\nabc.yandex.net");
            mock.verify();
        }, done).end();
    });
});
