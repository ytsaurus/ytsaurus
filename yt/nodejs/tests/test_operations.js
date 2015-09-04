var Q = require("bluebird");

var YtApplicationOperations = require("../lib/application_operations").that;

////////////////////////////////////////////////////////////////////////////////

var nock = require("nock");

////////////////////////////////////////////////////////////////////////////////

var OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive/ordered_by_id";
var OPERATIONS_INDEX_PATH = "//sys/operations_archive/ordered_by_start_time";

describe("YtApplicationOperations - list, get operations and scheduling info", function() {
    beforeEach(function(done) {
        this.driver = { executeSimple: function(){ return Q.resolve(); } };
        this.application_operations = new YtApplicationOperations(this.driver);
        done();
    });

    function createMocks(mock, runtime_available, archive_available) {
        var archive_result = archive_available ? Q.resolve([]) : Q.reject("Archive is unavalilable");

        mock
            .expects("executeSimple")
            .once()
            .withExactArgs("select_rows", sinon.match(function (params) {
                return params.hasOwnProperty("query") && params.query.match(/user, state, type, sum\(1\) as count .* group by authenticated_user as user, state, operation_type as type/);
            }))
            .returns(archive_result);

        mock
            .expects("executeSimple")
            .once()
            .withExactArgs("select_rows", sinon.match({
                query: sinon.match(/\* .* order by finish_time desc limit .*/)
            }))
            .returns(archive_result);
        
        var runtime_result = runtime_available ? Q.resolve([]) : Q.reject("Runtime data is unavalilable");
        mock
            .expects("executeSimple")
            .once()
            .withExactArgs("get", sinon.match({
                path: "//sys/scheduler/orchid/scheduler/operations"
            }))
            .returns(runtime_result);

        mock
            .expects("executeSimple")
            .once()
            .withExactArgs("list", sinon.match({
                path: "//sys/operations",
                attributes: sinon.match.any
            }))
            .returns(Q.resolve([]));
    }

    var empty_operations_list = {
        operations: [],
        user_counts: {
            "all": 0,
            "personal": 0,
            "other": 0
        },
        state_counts: {
            "all": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "aborted": 0
        },
        type_counts: {
            "all": 0,
            "map": 0,
            "map_reduce": 0,
            "merge": 0,
            "reduce": 0,
            "remote_copy": 0,
            "sort": 0
        },
        failed_jobs_count: 0,
        users: { }
    };

    it("should list oprations from cypress + orchid + archive)", function(done) {
        var driver = this.driver;
        var mock = sinon.mock(driver);
        createMocks(mock, true, true);
        this.application_operations.list({filter: ""}).then(function(result) {
            result.should.deep.equal(empty_operations_list);
            mock.verify();
            done();
        });
    });

    it("should list oprations from cypress + orchid", function(done) {
        var driver = this.driver;
        var mock = sinon.mock(driver);
        createMocks(mock, false, true);
        this.application_operations.list({filter: ""}).then(function(result) {
            result.should.deep.equal(empty_operations_list);
            mock.verify();
            done();
        });
    });

    it("should list oprations from cypress", function(done) {
        var driver = this.driver;
        var mock = sinon.mock(driver);
        createMocks(mock, false, false);
        this.application_operations.list({filter: ""}).then(function(result) {
            result.should.deep.equal(empty_operations_list);
            mock.verify();
            done();
        });
    });
});

