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

            for (var i = 0, length = names.length; i < length; ++i) {
                var name = names[i];
                var vesion_data = result[name];

                var request_mock = mock
                    .expects("executeSimple")
                    .once()
                    .withExactArgs("get", sinon.match({
                        path: "//sys/" + entity + "/" + name + "/orchid/service"
                    }));

                if (vesion_data != null) {
                    request_mock.returns(Q.resolve(vesion_data));
                } else {
                    request_mock.returns(Q.reject("Some error from orchid"));
                }
            }

            return result;
        }

        function createMock2(entity, result) {
            var names = Object.keys(result);

            makeNamesList(entity, names); 

            for (var i = 0, length = names.length; i < length; ++i) {
                var name = names[i];
                var vesion_data = result[name];

                if (vesion_data != null) {
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
            "primary_masters": createMock("primary_masters", {
                "master1": {
                    "version": "1"
                },
                "master2": null
            }),
            "secondary_masters": createMock("secondary_masters", {
                "1002/master1": {
                    "version": "1"
                },
                "1002/master2": null
            }),
            "nodes": createMock("nodes", {
                "node1": {
                    "version": "2"
                },
                "node2": {
                    "version": "3"
                },
                "node3": null
            }),
            "schedulers": createMock("scheduler/instances", { }),
            "proxies": createMock2("proxies", {
                "proxy1": {
                    "version": "1"
                },
                "proxy2": null
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

