var lru_cache = require("lru-cache");
var qs = require("querystring");

var YtDriver = require("../lib/driver").that;
var YtCommand = require("../lib/command").that;

var utils = require("../lib/utils");

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;
var die = require("./common_http").die;

// This will spawn a (mock of a) real API server.
function spawnServer(driver, watcher, done) {
    var logger = stubLogger();
    var coordinator = stubCoordinator();
    var rate_check_cache = lru_cache({ max: 5, maxAge: 5000 });
    return srv(function(req, rsp) {
        var pause = utils.Pause(req);
        req.authenticated_user = "root";
        return (new YtCommand(
            logger, driver, coordinator, watcher, rate_check_cache, pause
        )).dispatch(req, rsp);
    }, done);
}

// This stub provides a coordinator mock.
function stubCoordinator()
{
    return {
        getControlProxy: function() { return "localhost"; },
        getDataProxy: function() { return "localhost"; },
        getSelf: function() {
            return {
                host: "localhost",
                role: "data",
                banned: false,
                liveness: {},
                randomness: 0.0,
                fitness: 0.0
            };
        }
    };
}

// This stub provides a real driver instance which simply pipes all data through.
function stubDriver(echo)
{
    var config = {
        "low_watermark": 100,
        "high_watermark": 200,
        "proxy": {
            "driver": { "masters": { "addresses": [ "localhost:0" ] } },
            "logging": { "rules": [], "writers": {} }
        }
    };

    return new YtDriver(config, !!echo);
}

// This stub provides a constant watcher which is either choking or not.
function stubWatcher(is_choking) {
    return { tackle: function(){}, is_choking: function() { return is_choking; } };
}

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - http method selection", function() {
    var V = "/v2";

    before(function(done) {
        this.server = spawnServer(stubDriver(true), stubWatcher(false), done);
    });

    after(function(done) {
        die(this.server, done);
        this.server = null;
    });

    [ "/get", "/download", "/read" ]
    .forEach(function(entry_point) {
        it("should use GET for " + entry_point, function(done) {
            ask("GET", V + entry_point, {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });
    });

    [ "/set", "/upload", "/write" ]
    .forEach(function(entry_point) {
        it("should use PUT for " + entry_point, function(done) {
            ask("PUT", V + entry_point, {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });
    });

    [ "/map", "/reduce", "/sort" ]
    .forEach(function(entry_point) {
        it("should use POST for " + entry_point, function(done) {
            ask("POST", V + entry_point, {},
            function(rsp) { rsp.should.be.http2xx; }, done).end("{}");
        });
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - command name", function() {
    var V = "/v2";

    before(function(done) {
        this.server = spawnServer(stubDriver(true), stubWatcher(false), done);
    });

    after(function(done) {
        die(this.server, done);
        this.server = null;
    });

    it("should allow good names", function(done) {
        ask("GET", V + "/get", {},
        function(rsp) { rsp.should.be.http2xx; }, done).end();
    });

    it("should disallow bad names", function(done) {
        ask("GET", V + "/$$$", {},
        function(rsp) {
            rsp.statusCode.should.eql(400);
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should return 404 when the name is unknown", function(done) {
        ask("GET", V + "/unknown_but_valid_name", {},
        function(rsp) {
            rsp.statusCode.should.eql(404);
            rsp.should.be.yt_error;
        }, done).end();
    });

    [ "/v1", "/v2" ].forEach(function(version) {
    it("should display a reference when the name is empty", function(done) {
        ask("GET", version, {},
        function(rsp) {
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
        }, done).end();
    });
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - command heaviness", function() {
    var V = "/v2";

    before(function() {
        this.driver = stubDriver(true);
    });

    [ "download", "upload", "read", "write" ]
    .forEach(function(name) {
        it("should affect '" + name + "'", function() {
            this.driver.find_command_descriptor(name).is_heavy.should.be.true;
        });
    });

    describe("when there is no workload", function() {
        before(function(done) {
            this.server = spawnServer(this.driver, stubWatcher(false), done);
        });

        after(function(done) {
            die(this.server, done);
            this.server = null;
        });

        it("should allow light commands ", function(done) {
            ask("GET", V + "/get", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });

        it("should allow heavy commands ", function(done) {
            ask("GET", V + "/read", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });
    });

    describe("when there is workload", function() {
        before(function(done) {
            this.server = spawnServer(this.driver, stubWatcher(true), done);
        });

        after(function(done) {
            die(this.server, done);
            this.server = null;
        });

        it("should allow light commands ", function(done) {
            ask("GET", V + "/get", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });

        it("should disallow heavy commands ", function(done) {
            ask("GET", V + "/read", {},
            function(rsp) { rsp.statusCode.should.eql(503); }, done).end();
        });
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - command parameters", function() {
    var V = "/v2";

    beforeEach(function(done) {
        this.driver = stubDriver(true);
        this.server = spawnServer(this.driver, stubWatcher(false), done);
        this.stub   = sinon.spy(this.driver, "execute");
    });

    afterEach(function(done) {
        die(this.server, done);
        this.driver = null;
        this.server = null;
        this.stub   = null;
    });

    it("should set meaningful defaults", function(done) {
        var stub = this.stub;
        ask("GET", V + "/get",
        {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.empty;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].Get().should.eql({
                input_format: "yson",
                output_format: "json"
            });
        }, done).end();
    });

    it("should take query string parameters", function(done) {
        var stub = this.stub;
        var params = {
            "input_format": "yson",
            "output_format": "json",
            "who": "me",
            "path": "/",
            "foo": "bar"
        };
        ask("GET", V + "/get?" + qs.encode(params),
        {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.empty;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].Get().should.eql(params);
        }, done).end();
    });

    it("should take header parameters", function(done) {
        var stub = this.stub;
        var params = {
            "input_format": "yson",
            "output_format": "json",
            "who": "me",
            "path": "/",
            "foo": "bar"
        };
        ask("GET", V + "/get",
        { "X-YT-Parameters": JSON.stringify(params) },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.empty;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].Get().should.eql(params);
        }, done).end();
    });

    it("should take body parameters for POST methods", function(done) {
        var stub = this.stub;
        var params = {
            "input_format": "yson",
            "output_format": "json",
            "who": "me",
            "path": "/",
            "foo": "bar"
        };
        ask("POST", V + "/map",
        { "Content-Type": "application/json" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.empty;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].Get().should.eql(params);
        }, done).end(JSON.stringify(params));
    });

    it("should set proper precedence", function(done) {
        //  URL: a1 a2 a3
        // HEAD:    a2 a3 a4
        // BODY:       a3 a4 a5
        var stub = this.stub;
        var from_url  = qs.encode({ a1: "foo", a2: "bar", a3: "baz" });
        var from_head = JSON.stringify({ a2: "xyz", a3: "www", a4: "abc" });
        var from_body = JSON.stringify({ a3: "pooh", a4: "puff", a5: "blah" });
        ask("POST", V + "/map?" + from_url,
        { "Content-Type": "application/json", "X-YT-Parameters": from_head },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.empty;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].Get().should.eql({
                a1: "foo", a2: "xyz", a3: "pooh", a4: "puff", a5: "blah",
                input_format: "json", output_format: "json"
            });
        }, done).end(from_body);
    });

    it("should properly treat attributes in JSON", function(done) {
        var stub = this.stub;
        ask("POST", V + "/map",
        { "Content-Type": "application/json" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.empty;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].Print().should.eql('{"output_format"="json";"input_format"="json";"path"=<"append"="true">"//home"}');
        }, done).end('{"path":{"$value":"//home","$attributes":{"append":"true"}}}');
    });

    it("should properly treat binary strings in JSON", function(done) {
        var stub = this.stub;
        ask("POST", V + "/map",
        { "Content-Type": "application/json" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.body.should.be.empty;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].Print().should.eql('{"output_format"="json";"input_format"="json";"\\x80"="\\xFF"}');

        }, done).end('{"\\u0080":"\\u00FF"}');
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - input format selection", function() {
    var V = "/v2";

    beforeEach(function(done) {
        this.driver = stubDriver(true);
        this.server = spawnServer(this.driver, stubWatcher(false), done);
        this.stub   = sinon.spy(this.driver, "execute");
    });

    afterEach(function(done) {
        die(this.server, done);
        this.driver = null;
        this.server = null;
        this.stub   = null;
    });

    it("should use 'json' as a default for structured data", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/set", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('"json"');
        }, done).end();
    });

    it("should use 'yson' as a default for tabular data", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('<"format"="text">"yson"');
        }, done).end();
    });

    it("should use 'yson' as a default for binary data", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/upload", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('"yson"');
        }, done).end();
    });

    it("should respect Content-Type header", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write",
        { "Content-Type": "text/tab-separated-values" },
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('"dsv"');
        }, done).end();
    });

    it("should respect custom header with highest precedence and discard mime-type accordingly", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write",
        {
            "Content-Type": "text/tab-separated-values",
            "X-YT-Input-Format": JSON.stringify({
                $attributes: { "foo": "bar" },
                $value: "yson"
            })
        },
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('<"foo"="bar">"yson"');
        }, done).end();
    });

    it("should fail with bad Content-Type header", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write",
        { "Content-Type": "i-am-a-cool-hacker", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with bad X-YT-Input-Format header", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write",
        { "X-YT-Input-Format": "i-am-a-cool-hacker666{}[]", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with non-existent format", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write",
        { "X-YT-Input-Format": '"uberzoldaten"' },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - output format selection", function() {
    var V = "/v2";

    beforeEach(function(done) {
        this.driver = stubDriver(true);
        this.server = spawnServer(this.driver, stubWatcher(false), done);
        this.stub   = sinon.spy(this.driver, "execute");
    });

    afterEach(function(done) {
        die(this.server, done);
        this.driver = null;
        this.server = null;
        this.stub   = null;
    });

    it("should use application/json as a default for structured data", function(done) {
        var stub = this.stub;
        ask("GET", V + "/get", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/json");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('"json"');
        }, done).end();
    });

    it("should use application/x-yt-yson-text as a default for tabular data", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/x-yt-yson-text");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('<"format"="text">"yson"');
        }, done).end();
    });

    it("should use application/octet-stream as a default for binary data", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/octet-stream");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('"yson"');
        }, done).end();
    });

    it("should respect Accept header", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read",
        { "Accept": "text/tab-separated-values" },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("text/tab-separated-values");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('"dsv"');
        }, done).end();
    });

    it("should respect custom header with highest precedence and discard mime-type accordingly", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read",
        {
            "Accept": "text/tab-separated-values",
            "X-YT-Output-Format": JSON.stringify({
                $attributes: { "foo": "bar" },
                $value: "yson"
            })
        },
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.not.have.content_type;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('<"foo"="bar">"yson"');
        }, done).end();
    });

    it("should fail with bad Accept header", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read",
        { "Accept": "i-am-a-cool-hacker", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with bad X-YT-Output-Format header", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read",
        { "X-YT-Output-Format": "i-am-a-cool-hacker666{}[]", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with non-existing format", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read",
        { "X-YT-Output-Format": '"uberzoldaten"' },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should specify content disposition for /download", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition;
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should specify content disposition for /read", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition;
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should use attachment disposition by default", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should use attachment disposition when user requested garbage", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download?disposition=garbage", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should use inline disposition when user requested 'inline'", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download?disposition=inline", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("inline");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should guess filename from the path", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download?path=//home/sandello/data", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment; filename=\"yt_home_sandello_data\"");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should override filename from the query", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download?path=//home/sandello/data&filename=data.txt", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment; filename=\"data.txt\"");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should (hacky) use text/plain + inline disposition for STDERRs", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download?path=//sys/operations/111/jobs/222/stderr", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("inline; filename=\"yt_sys_operations_111_jobs_222_stderr\"");
            rsp.should.have.content_type("text/plain");
            stub.should.have.been.calledOnce;
        }, done).end();
    });
});
