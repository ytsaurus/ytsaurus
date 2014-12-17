var events = require("events");
var os = require("os");
var util = require("util");

var Q = require("bluebird");

var YtAccrualFailureDetector = require("./accrual_failure_detector").that;
var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("X", "Coordinator");

function parseBoolean(x)
{
    return typeof(x) === "string" ? x === "true" : !!x;
}

////////////////////////////////////////////////////////////////////////////////

function YtCoordinatedHost(config, host)
{
    "use strict";

    var role = "data";
    var dead = true;
    var banned = false;
    var ban_message = "";
    var liveness = { updated_at: new Date(0), load_average: 0.0, failing: false };
    var randomness = Math.random();
    var dampening = 0.0;

    var afd = new YtAccrualFailureDetector(
        config.afd_window_size,
        config.afd_phi_threshold,
        config.heartbeat_drift,
        config.heartbeat_interval + config.heartbeat_drift,
        config.heartbeat_interval);

    var age = config.death_age;

    var self = this;

    Object.defineProperty(this, "host", {
        value: host,
        writable: false,
        enumerable: true
    });

    Object.defineProperty(this, "role", {
        get: function() { return role; },
        set: function(value) {
            if (value !== "control" && value !== "data") {
                throw new TypeError("Role has to be either 'control' or 'data'");
            }

            role = value;
        },
        enumerable: true
    });

    Object.defineProperty(this, "dead", {
        get: function() { return dead; },
        set: function(value) {
            var dead_before = dead;
            dead = parseBoolean(value);

            if (!dead_before && dead) {
                self.emit("dead");
            } else if (dead_before && !dead) {
                self.emit("alive");
            }
        },
        enumerable: true
    });

    Object.defineProperty(this, "banned", {
        get: function() { return banned; },
        set: function(value) {
            var banned_before = banned;
            banned = parseBoolean(value);

            if (!banned_before && banned) {
                self.emit("banned");
            } else if (banned_before && !banned) {
                self.emit("unbanned");
            }
        },
        enumerable: true
    });

    Object.defineProperty(this, "ban_message", {
        get: function() {
            if (ban_message.length == 0) {
                return undefined;
            } else {
                return ban_message;
            }
        },
        set: function(value) {
            ban_message = value + "";
        },
        enumerable: true
    });

    Object.defineProperty(this, "liveness", {
        get: function() { return liveness; },
        set: function(value) {
            if (typeof(liveness) !== "object") {
                throw new TypeError("Liveness has to be an object");
            }

            liveness.updated_at = new Date(value.updated_at);
            liveness.load_average = parseFloat(value.load_average);
            liveness.failing = parseBoolean(value.failing);

            randomness = Math.random();
            dampening = 0.0;

            afd.heartbeatTS(liveness.updated_at);

            this.dead = !liveness.failing && (new Date() - liveness.updated_at) > age;
        },
        enumerable: true
    });

    Object.defineProperty(this, "randomness", {
        get: function() {
            return randomness;
        },
        enumerable: true
    });

    Object.defineProperty(this, "dampening", {
        get: function() {
            return dampening;
        },
        set: function(value) {
            if (typeof(value) !== "number") {
                throw new TypeError("Dampening has to be a number");
            }
            dampening = value;
        },
        enumerable: true
    });

    Object.defineProperty(this, "afd_sample", {
        get: function() {
            return {
                length: afd.sample.length,
                mean: afd.sample.mean,
                stddev: afd.sample.stddev
            };
        },
        enumerable: true
    });

    Object.defineProperty(this, "afd_phi", {
        get: function() {
            return afd.phiTS();
        },
        enumerable: true
    });

    Object.defineProperty(this, "fitness", {
        get: function() {
            return 0.0 +
                config.fitness_la_coefficient  * liveness.load_average +
                config.fitness_phi_coefficient * afd.phiTS() +
                config.fitness_rnd_coefficient * randomness +
                config.fitness_dmp_coefficient * dampening;
        },
        enumerable: true
    });

    // Prevent 'undefined' property.
    this._events = {};
    events.EventEmitter.call(this);

    // Hide EventEmitter properties to clean up JSON.
    Object.defineProperty(this, "_events", { enumerable: false });
    Object.defineProperty(this, "_maxListeners", { enumerable: false });
    Object.defineProperty(this, "domain", { enumerable: false });
}

util.inherits(YtCoordinatedHost, events.EventEmitter);

function YtCoordinator(config, logger, driver, fqdn)
{
    "use strict";
    this.__DBG = __DBG.Tagged();

    this.config = config;
    this.logger = logger;
    this.driver = driver;

    this.fqdn = fqdn;
    this.host = new YtCoordinatedHost(this.config, this.fqdn);

    this.hosts = {};
    this.hosts[this.fqdn] = this.host;

    if (this.config.enable) {
        this.failure_count = 0;
        this.failure_at = new Date(0);

        this.sync_at = new Date(0);

        this.initialized = false;

        this.timer = setInterval(this._refresh.bind(this), this.config.heartbeat_interval);
        this.timer.unref && this.timer.unref();

        this._refresh(); // Fire |_refresh| ASAP to avoid empty host list.
    }

    this.__DBG("New");
}

YtCoordinator.prototype._initialize = function()
{
    "use strict";

    var self = this;
    var fqdn = self.fqdn;
    var path = "//sys/proxies/" + utils.escapeYPath(fqdn);

    return self.driver.executeSimple("create", {
        type: "map_node",
        path: path
    })
    .then(
    function(create) {
        var req1 = self.driver.executeSimple(
            "set",
            { path: path + "/@role" },
            "data");
        var req2 = self.driver.executeSimple(
            "set",
            { path: path + "/@banned" },
            "false");
        return Q.all([ req1, req2 ]);
    },
    function(error) {
        if (error.checkFor(501)) {
            self.logger.debug("Presence resumed from " + path);
            return;
        } else {
            return Q.reject(error);
        }
    })
    .then(function() {
        self.logger.debug("Presence initialized at " + path);
        self.initialized = true;

        return self._refresh();
    })
    .catch(function(err) {
        var error = YtError.ensureWrapped(err);
        self.logger.error(
            "An error occured while initializing coordination",
            // TODO(sandello): Embed.
            { error: error.toJson() });
    })
    .done();
};

YtCoordinator.prototype._refresh = function()
{
    "use strict";

    var self = this;
    var fqdn = self.fqdn;
    var path = "//sys/proxies/" + utils.escapeYPath(fqdn);

    var sync = Q.resolve();

    if (self.config.announce) {
        if (!self.initialized) {
            return self._initialize();
        }

        self.__DBG("Updating coordination information");

        var now = new Date();
        var failing = false;

        if (self.failure_count > self.config.failure_threshold) {
            self.failure_at = now;
        }

        if (now - self.failure_at < self.config.failure_timeout) {
            failing = true;
        }

        sync = self.driver.executeSimple("set", { path: path + "/@liveness" }, {
            updated_at: (new Date()).toISOString(),
            load_average: os.loadavg()[0],
            failing: failing ? "true" : "false",
        });
    }

    return sync
    .then(function() {
        // We are dropping failure count as soon as we pushed it to Cypress.
        self.failure_count = 0;

        // We are resetting timed as we have successfully reported to masters.
        self.sync_at = new Date();

        return self.driver.executeSimple("list", {
            path: "//sys/proxies",
            attributes: [ "role", "banned", "ban_message", "liveness" ]
        });
    })
    .then(function(entries) {
        entries.forEach(function(entry) {
            var host = utils.getYsonValue(entry);

            var ref = self.hosts[host];
            if (typeof(ref) === "undefined") {
                self.logger.info("Discovered a new proxy", { host: host });
                ref = new YtCoordinatedHost(self.config, host);
                self.hosts[host] = ref;

                ref.on("dead", function() {
                    self.logger.info("Marking proxy as dead", { host: host });
                });
                ref.on("alive", function() {
                    self.logger.info("Marking proxy as alive", { host: host });
                });
                ref.on("banned", function() {
                    self.logger.info("Proxy was banned", { host: banned });
                });
                ref.on("unbanned", function() {
                    self.logger.info("Proxy was unbanned", { host: unbanned });
                });
            }

            self.__DBG("Proxy '%s' has been updated to %j", host, entry);

            ref.role = utils.getYsonAttribute(entry, "role");
            ref.banned = utils.getYsonAttribute(entry, "banned");
            ref.ban_message = utils.getYsonAttribute(entry, "ban_message");
            ref.liveness = utils.getYsonAttribute(entry, "liveness");
        });
    })
    .catch(function(err) {
        // Re-run initialization next time, just in case.
        self.initialized = false;

        var error = YtError.ensureWrapped(err);
        self.logger.error(
            "An error occured while updating coordination",
            // TODO(sandello): Embed.
            { error: error.toJson() });
    })
    .done();
};

YtCoordinator.prototype.getProxies = function(role, dead, banned)
{
    "use strict";
    var result = [];
    for (var p in this.hosts) {
        if (this.hosts.hasOwnProperty(p)) {
            var ref = this.hosts[p];
            if (typeof(role) !== "undefined" && role !== ref.role) {
                continue;
            }
            if (typeof(dead) !== "undefined" && dead !== ref.dead) {
                continue;
            }
            if (typeof(banned) !== "undefined" && banned !== ref.banned) {
                continue;
            }
            result.push(this.hosts[p]);
        }
    }
    return result;
};

YtCoordinator.prototype.isSelfAlive = function()
{
    return (new Date() - this.sync_at) < this.config.death_age;
};

YtCoordinator.prototype.getSelf = function()
{
    return this.host;
};

YtCoordinator.prototype.countFailure = function()
{
    ++this.failure_count;
};

YtCoordinator.prototype.allocateDataProxy = function()
{
    "use strict";

    var victim = this
        .getProxies("data", false, false)
        .sort(function(lhs, rhs) { return lhs.fitness - rhs.fitness; })[0];

    if (typeof(victim) !== "undefined") {
        victim.dampening += 1;
    }

    return victim;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtCoordinator;
