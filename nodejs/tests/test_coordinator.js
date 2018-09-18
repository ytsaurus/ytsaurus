var Q = require("bluebird");

var YtCoordinator = require("../lib/coordinator").that;

////////////////////////////////////////////////////////////////////////////////

function stubCoordinatorConfig()
{
    return {
        enable: true,
        announce: true,
        heartbeat_interval: 5000,
        heartbeat_drift: 500,
        fitness_la_coefficient: 1,
        fitness_net_coefficient: 0.05,
        fitness_phi_coefficient: 6,
        fitness_rnd_coefficient: 1,
        fitness_dmp_coefficient: 0.3,
        afd_window_size: 24,
        afd_phi_threshold: 4,
        death_age: 300000,
    };
}

function stubDriver()
{
    return { executeSimple: function() { return Q.reject(
        "[" + arguments[0] + "; " +
            JSON.stringify(arguments[1]) + "; " +
            JSON.stringify(arguments[2]) + "]");
    } };
}

describe("YtCoordination", function() {
    beforeEach(function() {
        this.config = stubCoordinatorConfig();
        this.logger = stubLogger();
        this.driver = stubDriver();
    });

    it("should register itself in Cypress", function(done) {
        var mock = sinon.mock(this.driver);
        mock
            .expects("executeSimple")
            .once()
            .withExactArgs("create", sinon.match({
                  attributes: { banned: "false", role: "data" },
                  path: "//sys/proxies/myfqdn:12345",
                  type: "map_node"
            }))
            .returns(Q.resolve());

        var coordinator = new YtCoordinator(
            this.config, this.logger, this.driver,
            "myfqdn", 12345);

        coordinator.initDeferred.promise
            .then(function(val) {
                coordinator.stop();
                mock.verify();
                done(val);
            });
    });

    it("should sync up with others in Cypress", function(done) {
        var mock = sinon.stub(this.driver, "executeSimple");
        mock.returns(Q.resolve([
            {
                $attributes: {role: "data", liveness: {
                    updated_at: new Date().toISOString()
                }},
                $value: "hisfqdn:100500"
            }
        ]));

        var coordinator = new YtCoordinator(
            this.config, this.logger, this.driver,
            "myfqdn", 12345);

        coordinator.syncDeferred.promise
            .then(function() {}, function(err) { return err; })
            .then(function(val) {
                var proxies = coordinator.getProxies("data");
                expect(proxies).to.have.length(2);
                expect(proxies[0].name).to.eql("myfqdn:12345");
                expect(proxies[1].name).to.eql("hisfqdn:100500");
                coordinator.stop();
                done(val);
            });
    });

});

