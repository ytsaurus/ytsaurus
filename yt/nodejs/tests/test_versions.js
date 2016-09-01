var Q = require("bluebird");

var YtApplicationVersions = require("../lib/application_versions").that;

////////////////////////////////////////////////////////////////////////////////

var nock = require("nock");

////////////////////////////////////////////////////////////////////////////////

describe("YtApplicationVersions - discover versions", function() {
    beforeEach(function(done) {
        this.driver = { executeSimple: function(){ return Q.resolve(); } };
        this.application_versions = new YtApplicationVersions(this.driver);
        done();
    });

    it("should discover versions", function(done) {
        var driver = this.driver;
        var mock = sinon.mock(driver);

        function makeNamesList(entity, names) {
            var name_result = {};
            names.forEach(function (name) {
                var current = name_result;
                name.split("/").forEach(function (fragment) {
                    if (!current.hasOwnProperty(fragment)) {
                        current[fragment] = {};
                    }
                    current = current[fragment];
                });
            });
    
            mock
                .expects("executeSimple")
                .once()
                .withExactArgs("get", sinon.match({
                    path: "//sys/" + entity
                }))
                .returns(Q.resolve(name_result));
        }

        function createMock(entity, result) {
            var names = Object.keys(result);

            makeNamesList(entity, names); 

            var requests = [];
            var responses = [];
            for (var i = 0, length = names.length; i < length; ++i) {
                var name = names[i];
                var version_data = result[name];

                requests.push({
                    command: "get",
                    parameters: {
                        path: "//sys/" + entity + "/" + name + "/orchid/service"
                    }
                });

                if (!version_data.hasOwnProperty("error")) {
                    responses.push({output: {"$value": version_data }});
                } else {
                    responses.push({error: version_data.error});
                }
            }

            mock
                .expects("executeSimple")
                .once()
                .withExactArgs("execute_batch", sinon.match({
                    requests: requests
                }))
                .returns(Q.resolve(responses));

            return result;
        }

        function createMock2(entity, result) {
            var names = Object.keys(result);

            makeNamesList(entity, names); 

            for (var i = 0, length = names.length; i < length; ++i) {
                var name = names[i];
                var version_data = result[name];

                if (!version_data.hasOwnProperty("error")) {
                    nock("http://" + name)
                        .get("/service")
                        .reply(200, version_data);
                } else {
                    nock("http://" + name)
                        .get("/service")
                        .reply(503);
                }
            }

            return result;
        }

        var error_from_orchid = {"error":{"code":-2,"message":"Some error from orchid","attributes":{},"inner_errors":[]}};

        var versions = {
            "primary_masters": createMock("primary_masters", {
                "master1": {
                    "version": "1"
                },
                "master2": error_from_orchid
            }),
            "secondary_masters": createMock("secondary_masters", {
                "1002/master1": {
                    "version": "1"
                },
                "1002/master2": error_from_orchid
            }),
            "nodes": createMock("nodes", {
                "node1": {
                    "version": "2"
                },
                "node2": {
                    "version": "3"
                },
                "node3": error_from_orchid
            }),
            "schedulers": createMock("scheduler/instances", { }),
            "proxies": createMock2("proxies", {
                "proxy1": {
                    "version": "1"
                },
                "proxy2": {"error":{"code":-2,"message":"Request to \'proxy2:80/service\' has responded with 503","attributes":{},"inner_errors":[]}}
            })
        };

        var application_versions = this.application_versions;

        application_versions.get_versions().then(function(result) {
            JSON.stringify(result).should.equal(JSON.stringify(versions));
            mock.verify();
        })
        .then(done, done);
    });
});

