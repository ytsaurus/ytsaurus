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

        function createMock(entity, result) {
            var names = Object.keys(result);

            mock
                .expects("executeSimple")
                .once()
                .withExactArgs("list", sinon.match({
                    path: "//sys/" + entity
                }))
                .returns(Q.resolve(names));

            for (var i = 0, length = names.length; i < length; ++i) {
                var name = names[i];
                var vesion_data = result[name];

                var request_mock = mock
                    .expects("executeSimple")
                    .once()
                    .withExactArgs("get", sinon.match({
                        path: "//sys/" + entity + "/" + name + "/orchid/service"
                    }));

                if (!vesion_data.hasOwnProperty("error")) {
                    request_mock.returns(Q.resolve(vesion_data));
                } else {
                    request_mock.returns(Q.reject("Some error from orchid"));
                }
            }

            return result;
        }

        function createMock2(entity, result) {
            var names = Object.keys(result);

            mock
                .expects("executeSimple")
                .once()
                .withExactArgs("list", sinon.match({
                    path: "//sys/" + entity
                }))
                .returns(Q.resolve(names));

            for (var i = 0, length = names.length; i < length; ++i) {
                var name = names[i];
                var vesion_data = result[name];

                if (!vesion_data.hasOwnProperty("error")) {
                    nock("http://" + name)
                        .get("/version")
                        .reply(200, vesion_data["version"]);
                } else {
                    nock("http://" + name)
                        .get("/version")
                        .reply(503);
                }
            }

            return result;
        }

        var versions = {
            "masters": createMock("masters", {
                "master1": {
                    "version": "1"
                },
                "master2": {
                    "error": "Some error from orchid"
                }
            }),
            "nodes": createMock("nodes", {
                "node1": {
                    "version": "2"
                },
                "node2": {
                    "version": "3"
                },
                "node3": {
                    "error": "Some error from orchid"
                }
            }),
            "schedulers": createMock("scheduler/instances", { }),
            "proxies": createMock2("proxies", {
                "proxy1": {
                    "version": "1"
                },
                "proxy2": {"error":{"code":-2,"message":"Request to \'proxy2:80/version\' has responded with 503","attributes":{},"inner_errors":[]}}
            })
        };

        var application_versions = this.application_versions;

        application_versions.get_versions().then(function(result) {
            JSON.stringify(result).should.equal(JSON.stringify(versions));
            mock.verify();
            done();
        });
    });
});

