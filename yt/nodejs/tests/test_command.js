var lru_cache = require("lru-cache");
var qs = require("querystring");
var Q = require("bluebird");

var YtDriver = require("../lib/driver").that;
var YtError = require("../lib/error").that;
var YtCommand = require("../lib/command").that;

var binding = require("../lib/ytnode");
var utils = require("../lib/utils");

////////////////////////////////////////////////////////////////////////////////

var ask = require("./common_http").ask;
var srv = require("./common_http").srv;
var die = require("./common_http").die;

// This will spawn a (mock of a) real API server.
function spawnServer(driver, coordinator, watcher, done) {
    var logger = stubLogger();
    var sticky_cache = lru_cache({ max: 5, maxAge: 5000 });
    return srv(function(req, rsp) {
        var pause = utils.Pause(req);
        req.authenticated_user = "root";
        return (new YtCommand(
            logger, driver, coordinator, watcher, sticky_cache, pause
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

// This stub provides a constant watcher which is either choking or not.
function stubWatcher() {
    return {
        isChoking: function() { return false; },
        acquireThread: function() { return true; },
        releaseThread: function() {},
    };
}

// This stub provides a real driver instance which simply pipes all data through.
function stubDriver()
{
    var config = {
        "low_watermark": 100,
        "high_watermark": 200,
        "proxy": {
            "driver": {
                "master": {
                    "cell_tag" : 0,
                    "cell_id" : "ffffffff-ffffffff-ffffffff-ffffffff",
                    "addresses": [ ]
                }
            },
            "tracing": {
            },
            "logging": {
                "rules": [],
                "writers": {}
            }
        }
    };

    return new YtDriver(config, /* echo */ true);
}

function beforeCommandTest(done) {
    this.driver = stubDriver();
    this.watcher = stubWatcher();
    this.coordinator = stubCoordinator();
    this.server = spawnServer(this.driver, this.coordinator, this.watcher, done);
}

function afterCommandTest(done) {
    die(this.server, done);
    this.server = null;
    this.coordinator = null;
    this.watcher = null;
    this.driver = null;
}

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - v2 http method selection", function() {
    var V = "/v2";

    before(beforeCommandTest);
    after(afterCommandTest);

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

    [ "/map", "/reduce", "/sort", "/merge", "/map_reduce" ]
    .forEach(function(entry_point) {
        it("should use POST for " + entry_point, function(done) {
            ask("POST", V + entry_point, {},
            function(rsp) { rsp.should.be.http2xx; }, done).end("{}");
        });
    });
});

describe("YtCommand - v3 http method selection", function() {
    var V = "/v3";

    before(beforeCommandTest);
    after(afterCommandTest);

    [ "/get", "/read_file", "/read_table", "/read_journal" ]
    .forEach(function(entry_point) {
        it("should use GET for " + entry_point, function(done) {
            ask("GET", V + entry_point, {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });
    });

    [ "/set", "/write_file", "/write_table", "/write_journal" ]
    .forEach(function(entry_point) {
        it("should use PUT for " + entry_point, function(done) {
            ask("PUT", V + entry_point, {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });
    });

    [ "/map", "/reduce", "/sort", "/merge", "/map_reduce" ]
    .forEach(function(entry_point) {
        it("should use POST for " + entry_point, function(done) {
            ask("POST", V + entry_point, {},
            function(rsp) { rsp.should.be.http2xx; }, done).end("{}");
        });
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - v2 command name", function() {
    var V = "/v2";

    before(beforeCommandTest);
    after(afterCommandTest);

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
});

describe("YtCommand - v3 command name", function() {
    var V = "/v3";

    before(beforeCommandTest);
    after(afterCommandTest);

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
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - command descriptors", function() {
    before(beforeCommandTest);
    after(afterCommandTest);

    it("should return a list of supported versions", function(done) {
        ask("GET", "/", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/json");
            var body = JSON.parse(rsp.body);
            body.should.be.instanceof(Array);
            body.should.have.members(["v2", "v3"]);
        }, done).end();
    });

    [ "/v2", "/v3" ].forEach(function(version) {
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

    it("should return proper methods for /v2", function(done) {
        var expected_methods = [
            'abort_op',
            'abort_tx',
            'add_member',
            'check_permission',
            'commit_tx',
            'copy',
            'create',
            'download',
            'erase',
            'exists',
            'get',
            'get_version',
            'link',
            'list',
            'lock',
            'map',
            'map_reduce',
            'merge',
            'move',
            'parse_ypath',
            'ping_tx',
            'read',
            'reduce',
            'remote_copy',
            'remove',
            'remove_member',
            'resume_op',
            'set',
            'sort',
            'start_tx',
            'suspend_op',
            'upload',
            'write',
        ];
        ask("GET", "/v2", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/json");
            var body = JSON.parse(rsp.body);
            body.should.be.instanceof(Array);
            body
                .map(function(item) { return item.name; })
                .should.have.members(expected_methods);
        }, done).end();
    });

    it("should return proper methods for /v3", function(done) {
        var expected_methods = [
            'abort_op',
            'abort_tx',
            'add_member',
            'check_permission',
            'commit_tx',
            'copy',
            'create',
            'delete_rows',
            'dump_job_context',
            'erase',
            'exists',
            'get',
            'get_version',
            'insert_rows',
            'link',
            'list',
            'lock',
            'lookup_rows',
            'map',
            'map_reduce',
            'merge',
            'mount_table',
            'move',
            'parse_ypath',
            'ping_tx',
            'read_file',
            'read_journal',
            'read_table',
            'reduce',
            'remote_copy',
            'remount_table',
            'remove',
            'remove_member',
            'reshard_table',
            'resume_op',
            'select_rows',
            'set',
            'sort',
            'start_tx',
            'strace_job',
            'suspend_op',
            'unmount_table',
            'write_file',
            'write_journal',
            'write_table',
            '_discover_versions',
            '_list_operations',
            '_get_operation',
            '_get_scheduling_information',
        ];
        ask("GET", "/v3", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/json");
            var body = JSON.parse(rsp.body);
            body.should.be.instanceof(Array);
            body
                .map(function(item) { return item.name; })
                .should.have.members(expected_methods);
        }, done).end();
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - v2 command heaviness", function() {
    var V = "/v2";

    before(beforeCommandTest);
    after(afterCommandTest);

    describe("when there is no workload", function() {
        it("should allow light commands", function(done) {
            ask("GET", V + "/get", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });

        it("should allow heavy commands", function(done) {
            ask("GET", V + "/read", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });
    });

    describe("when there is workload", function() {
        before(function() {
            sinon.stub(this.watcher, "isChoking").returns(true);
        });

        after(function() {
            this.watcher.isChoking.restore();
        });

        it("should allow light commands", function(done) {
            ask("GET", V + "/get", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });

        it("should disallow heavy commands", function(done) {
            ask("GET", V + "/read", {},
            function(rsp) { rsp.statusCode.should.eql(503); }, done).end();
        });
    });
});

describe("YtCommand - v3 command heaviness", function() {
    var V = "/v3";

    before(beforeCommandTest);
    after(afterCommandTest);

    describe("when there is no workload", function() {
        it("should allow light commands ", function(done) {
            ask("GET", V + "/get", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });

        it("should allow heavy commands ", function(done) {
            ask("GET", V + "/read_table", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });
    });

    describe("when there is workload", function() {
        before(function() {
            sinon.stub(this.watcher, "isChoking").returns(true);
        });

        after(function() {
            this.watcher.isChoking.restore();
        });

        it("should allow light commands ", function(done) {
            ask("GET", V + "/get", {},
            function(rsp) { rsp.should.be.http2xx; }, done).end();
        });

        it("should disallow heavy commands ", function(done) {
            ask("GET", V + "/read_table", {},
            function(rsp) { rsp.statusCode.should.eql(503); }, done).end();
        });
    });
});

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - v2 command parameters", function() {
    var V = "/v2";

    before(beforeCommandTest);
    after(afterCommandTest);

    beforeEach(function() {
        this.stub = sinon.spy(this.driver, "execute");
    });

    afterEach(function() {
        this.stub.restore();
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

    it("should take header parameters in YSON", function(done) {
        var stub = this.stub;
        var params = {
            "input_format": "yson",
            "output_format": "json",
            "who": "me",
            "foo": "bar"
        };
        ask("GET", V + "/get",
        { "X-YT-Header-Format": "yson", "X-YT-Parameters": "{who=me;foo=bar}" },
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
            stub.firstCall.args[6].Print()
                .should.eql('{"output_format"=<"boolean_as_string"=%true>"json";"input_format"=<"boolean_as_string"=%true>"json";"path"=<"append"="true">"//home"}');
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
            stub.firstCall.args[6].Print()
                .should.eql('{"output_format"=<"boolean_as_string"=%true>"json";"input_format"=<"boolean_as_string"=%true>"json";"\\x80"="\\xFF"}');

        }, done).end('{"\\u0080":"\\u00FF"}');
    });
});

describe("YtCommand - v3 command parameters", function() {
    var V = "/v3";

    before(beforeCommandTest);
    after(afterCommandTest);

    beforeEach(function() {
        this.stub = sinon.spy(this.driver, "execute");
    });

    afterEach(function() {
        this.stub.restore();
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

describe("YtCommand - v2 input format selection", function() {
    var V = "/v2";

    before(beforeCommandTest);
    after(afterCommandTest);

    beforeEach(function() {
        this.stub = sinon.spy(this.driver, "execute");
    });

    afterEach(function() {
        this.stub.restore();
    });

    it("should use 'json' as a default for structured data", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/set", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print()
                .should.eql('<"boolean_as_string"=%true>"json"');
        }, done).end();
    });

    it("should use 'yson' as a default for tabular data", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            var ifmt = stub.firstCall.args[6].GetByYPath("/input_format").Print();
            expect([
                '<"format"="text";"boolean_as_string"=%true>"yson"',
                '<"boolean_as_string"=%true;"format"="text">"yson"'
            ]).to.include(ifmt);
        }, done).end();
    });

    it("should use 'yson' as a default for binary data", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/upload", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print()
                .should.eql('<"boolean_as_string"=%true>"yson"');
        }, done).end();
    });

    it("should respect Content-Type header", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write",
        { "Content-Type": "text/tab-separated-values" },
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print()
                .should.eql('<"boolean_as_string"=%true>"dsv"');
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
            var ifmt = stub.firstCall.args[6].GetByYPath("/input_format").Print();
            expect([
                '<"foo"="bar";"boolean_as_string"=%true>"yson"',
                '<"boolean_as_string"=%true;"foo"="bar">"yson"'
            ]).to.include(ifmt);
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

describe("YtCommand - v2 output format selection", function() {
    var V = "/v2";

    before(beforeCommandTest);
    after(afterCommandTest);

    beforeEach(function() {
        this.stub = sinon.spy(this.driver, "execute");
    });

    afterEach(function() {
        this.stub.restore();
    });

    it("should use application/json as a default for structured data", function(done) {
        var stub = this.stub;
        ask("GET", V + "/get", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/json");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print()
                .should.eql('<"boolean_as_string"=%true>"json"');
        }, done).end();
    });

    it("should use application/x-yt-yson-text as a default for tabular data", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/x-yt-yson-text");
            stub.should.have.been.calledOnce;
            var ofmt = stub.firstCall.args[6].GetByYPath("/output_format").Print();
            expect([
                '<"boolean_as_string"=%true;"format"="text">"yson"',
                '<"format"="text";"boolean_as_string"=%true>"yson"',
            ]).to.include(ofmt);
        }, done).end();
    });

    it("should use application/octet-stream as a default for binary data", function(done) {
        var stub = this.stub;
        ask("GET", V + "/download", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/octet-stream");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print()
                .should.eql('<"boolean_as_string"=%true>"yson"');
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
            stub.firstCall.args[6].GetByYPath("/output_format").Print()
                .should.eql('<"boolean_as_string"=%true>"dsv"');
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
            var ofmt = stub.firstCall.args[6].GetByYPath("/output_format").Print();
            expect([
                '<"foo"="bar";"boolean_as_string"=%true>"yson"',
                '<"boolean_as_string"=%true;"foo"="bar">"yson"',
            ]).to.include(ofmt);
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

////////////////////////////////////////////////////////////////////////////////

describe("YtCommand - v3 input format selection", function() {
    var V = "/v3";

    before(beforeCommandTest);
    after(afterCommandTest);

    beforeEach(function() {
        this.stub = sinon.spy(this.driver, "execute");
    });

    afterEach(function() {
        this.stub.restore();
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

    it("should use 'yson' as a default for journals", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write_journal", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('<"format"="text">"yson"');
        }, done).end();
    });

    it("should use 'yson' as a default for tables", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write_table", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('<"format"="text">"yson"');
        }, done).end();
    });

    it("should use 'yson' as a default for files", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write_file", {},
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('"yson"');
        }, done).end();
    });

    it("should respect Content-Type header", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write_table",
        { "Content-Type": "text/tab-separated-values" },
        function(rsp) {
            rsp.should.be.http2xx;
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/input_format").Print().should.eql('"dsv"');
        }, done).end();
    });

    it("should respect custom header with highest precedence and discard mime-type accordingly", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write_table",
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
        ask("PUT", V + "/write_table",
        { "Content-Type": "i-am-a-cool-hacker", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with bad X-YT-Input-Format header", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write_table",
        { "X-YT-Input-Format": "i-am-a-cool-hacker666{}[]", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with non-existent format", function(done) {
        var stub = this.stub;
        ask("PUT", V + "/write_table",
        { "X-YT-Input-Format": '"uberzoldaten"' },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });
});

describe("YtCommand - v3 output format selection", function() {
    var V = "/v3";

    before(beforeCommandTest);
    after(afterCommandTest);

    beforeEach(function() {
        this.stub = sinon.spy(this.driver, "execute");
    });

    afterEach(function() {
        this.stub.restore();
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

    it("should use application/x-yt-yson-text as a default for journals", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_journal", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/x-yt-yson-text");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('<"format"="text">"yson"');
        }, done).end();
    });

    it("should use application/x-yt-yson-text as a default for tables", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_table", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/x-yt-yson-text");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('<"format"="text">"yson"');
        }, done).end();
    });

    it("should use application/octet-stream as a default for files", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_type("application/octet-stream");
            stub.should.have.been.calledOnce;
            stub.firstCall.args[6].GetByYPath("/output_format").Print().should.eql('"yson"');
        }, done).end();
    });

    it("should respect Accept header", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_table",
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
        ask("GET", V + "/read_table",
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
        ask("GET", V + "/read_table",
        { "Accept": "i-am-a-cool-hacker", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with bad X-YT-Output-Format header", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_table",
        { "X-YT-Output-Format": "i-am-a-cool-hacker666{}[]", },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should fail with non-existing format", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_table",
        { "X-YT-Output-Format": '"uberzoldaten"' },
        function(rsp) {
            rsp.should.be.yt_error;
        }, done).end();
    });

    it("should specify content disposition for /read_file", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition;
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should specify content disposition for /read_table", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition;
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should specify content disposition for /read_journal", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_journal", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition;
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should use attachment disposition by default", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should use attachment disposition when user requested garbage", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file?disposition=garbage", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should use inline disposition when user requested 'inline'", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file?disposition=inline", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("inline");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should guess filename from the path", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file?path=//home/sandello/data", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment; filename=\"yt_home_sandello_data\"");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should override filename from the query", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file?path=//home/sandello/data&filename=data.txt", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("attachment; filename=\"data.txt\"");
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should (hacky) use text/plain + inline disposition for STDERRs", function(done) {
        var stub = this.stub;
        ask("GET", V + "/read_file?path=//sys/operations/111/jobs/222/stderr", {},
        function(rsp) {
            rsp.should.be.http2xx;
            rsp.should.have.content_disposition("inline; filename=\"yt_sys_operations_111_jobs_222_stderr\"");
            rsp.should.have.content_type("text/plain");
            stub.should.have.been.calledOnce;
        }, done).end();
    });
});

describe("YtCommand - specific behaviour", function() {
    var V = "/v2";

    beforeEach(beforeCommandTest);
    afterEach(afterCommandTest);

    it("should reply with 503 on AllTargetNodesFailed error", function(done) {
        var stub = sinon.stub(this.driver, "execute");
        stub.returns(Q.reject(
            new YtError("YTADMIN-1685").withCode(binding.AllTargetNodesFailedYtErrorCode)
        ));
        ask("PUT", V + "/write", {}, function(rsp) {
            rsp.statusCode.should.eql(503);
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should reply with 503 on Unavailable error", function(done) {
        var stub = sinon.stub(this.driver, "execute");
        stub.returns(Q.reject(
            new YtError("Unavailable").withCode(binding.UnavailableYtErrorCode)
        ));
        ask("PUT", V + "/write", {}, function(rsp) {
            rsp.statusCode.should.eql(503);
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should reply with 403 on UserBanned error", function(done) {
        var stub = sinon.stub(this.driver, "execute");
        stub.returns(Q.reject(
            new YtError("Banned").withCode(binding.UserBannedYtErrorCode)
        ));
        ask("PUT", V + "/write", {}, function(rsp) {
            rsp.statusCode.should.eql(403);
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should reply with 429 on RequestRateLimit error", function(done) {
        var stub = sinon.stub(this.driver, "execute");
        stub.returns(Q.reject(
            new YtError("RequestRateLimitExceeded").withCode(binding.RequestRateLimitExceededYtErrorCode)
        ));
        ask("PUT", V + "/write", {}, function(rsp) {
            rsp.statusCode.should.eql(429);
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should reply with 503 when proxy is banned", function(done) {
        var stub = sinon.stub(this.coordinator, "getSelf");
        stub.returns({ banned: true });
        ask("GET", V + "/get?path=/", {}, function(rsp) {
            rsp.statusCode.should.eql(503);
            stub.should.have.been.calledOnce;
        }, done).end();
    });

    it("should reply with 503 when there are no spare threads", function(done) {
        var stub = sinon.stub(this.watcher, "acquireThread");
        stub.returns(false);
        ask("GET", V + "/get?path=/", {}, function(rsp) {
            rsp.statusCode.should.eql(503);
            stub.should.have.been.calledOnce;
        }, done).end();
    });
});

