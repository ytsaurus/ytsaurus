var events = require("events");
var fs = require("fs");
var os = require("os");
var util = require("util");

var Q = require("bluebird");

var YtAccrualFailureDetector = require("./accrual_failure_detector").that;
var YtError = require("./error").that;
var YtReservoir = require("./reservoir").that;
var YtNetworkMeter = require("./network_meter").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("X", "Coordinator");

function parseBoolean(x)
{
    return typeof(x) === "string" ? x === "true" : !!x;
}

////////////////////////////////////////////////////////////////////////////////

function YtCoordinatedHost(config, name)
{
    var role = "data";
    var dead = true;
    var banned = false;
    var ban_message = "";
    var liveness = {
        updated_at: new Date(0),
        load_average: 0.0,
        network_load: {},
        network_coef: 0.0,
        network_coef_pow: 0.0,
    };
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

    Object.defineProperty(this, "name", {
        value: name,
        writable: false,
        enumerable: true
    });

    // COMPAT(sandello): Web-interface requires `host` for now.
    Object.defineProperty(this, "host", {
        value: name.split(":")[0],
        writable: false,
        enumerable: true
    });

    Object.defineProperty(this, "role", {
        get: function() { return role; },
        set: function(value) {
            if (typeof(value) !== "string") {
                throw new TypeError("Role must be string");
            }

            if (value === "") {
                throw new TypeError("Role must not be empty");
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
            return ban_message;
        },
        set: function(value) {
            if (typeof(value) === "undefined" || value === null) {
                ban_message = null;
            } else {
                ban_message = value + "";
            }
        },
        enumerable: true
    });

    Object.defineProperty(this, "liveness", {
        get: function() { return liveness; },
        set: function(value) {
            if (typeof(value) !== "object") {
                throw new TypeError("Liveness has to be an object");
            }

            liveness.updated_at = new Date(value.updated_at);
            liveness.load_average = parseFloat(value.load_average) || 0.0;
            liveness.network_load = value.network_load;
            liveness.network_coef = parseFloat(value.network_coef) || 0.0;
            liveness.network_coef_pow = 1.0 - Math.pow(1.0 - liveness.network_coef, 1.5);

            randomness = Math.random();
            dampening = 0.0;

            afd.heartbeatTS(liveness.updated_at);

            this.dead = (new Date() - liveness.updated_at) > age;
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
                config.fitness_net_coefficient * liveness.network_coef_pow +
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

function YtCoordinator(config, logger, driver, fqdn, port)
{
    this.__DBG = __DBG.Tagged();

    this.config = config;
    this.logger = logger;
    this.driver = driver;

    if (fqdn.indexOf(":") >= 0) {
        this.name = fqdn;
    } else {
        this.name = fqdn + ":" + port;
    }
    this.host = new YtCoordinatedHost(this.config, this.name);

    this.hosts = {};
    this.hosts[this.name] = this.host;

    this.announce = this.config.enable && this.config.announce;
    this.initialized = false;

    this.initDeferred = Q.defer();
    this.syncDeferred = Q.defer();

    if (this.config.enable) {
        this.sync_at = new Date(0);

        this.network_meter = new YtNetworkMeter(this.logger, this.config.net_window_size);

        this.timer = setInterval(this._refresh.bind(this), this.config.heartbeat_interval);
        if (this.timer && this.timer.unref) { this.timer.unref(); }

        this._refresh(); // Fire |_refresh| ASAP to avoid empty host list.
    } else {
        this.initDeferred.reject(new YtError("Coordination is disabled"));
        this.syncDeferred.reject(new YtError("Coordination is disabled"));
    }

    this.__DBG("New");
}

YtCoordinator.prototype.stop = function()
{
    if (this.timer) {
        clearInterval(this.timer);
        this.timer = null;
    }
};

YtCoordinator.prototype._initialize = function()
{
    var self = this;
    var name = self.name;
    var path = "//sys/proxies/" + utils.escapeYPath(name);

    return self.driver.executeSimple("create", {
        type: "map_node",
        path: path,
        attributes: {
            role: "data",
            banned: "false",
        },
    })
    .then(
    function() {
        self.logger.debug("Presence initialized at " + path);
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
        self.initialized = true;
        self.initDeferred.resolve();

        return self._refresh();
    })
    .catch(function(err) {
        var error = YtError.ensureWrapped(err);
        self.logger.error(
            "An error occurred while initializing coordination",
            // TODO(sandello): Embed.
            { error: error.toJson() });

        self.initDeferred.reject(err);
    })
    .done();
};

YtCoordinator.prototype._refresh = function()
{
    var self = this;
    var name = self.name;
    var path = "//sys/proxies/" + utils.escapeYPath(name);

    var sync = Q.resolve();

    self.__DBG("Refreshing coordination information");

    if (self.announce) {
        if (!self.initialized) {
            return void self._initialize();
        }

        self.__DBG("Announcing coordination information");

        var now = new Date();

        sync = self.network_meter.refresh()
        .then(function() {
            return self.driver.executeSimple("set", { path: path + "/@liveness" }, {
                updated_at: now.toISOString(),
                load_average: os.loadavg()[0],
                network_load: self.network_meter.load,
                network_coef: self.network_meter.coef,
            });
        });
    }

    var deferred = self.syncDeferred;
    self.syncDeferred = Q.defer();

    return sync
    .then(function() {
        // We are resetting timed as we have successfully reported to masters.
        self.sync_at = new Date();

        return self.driver.executeSimple("list", {
            path: "//sys/proxies",
            attributes: ["role", "banned", "ban_message", "liveness"]
        });
    })
    .then(function(entries) {
        __DBG("Got coordination information");
        entries.forEach(function(entry) {
            var otherName = utils.getYsonValue(entry);

            var ref = self.hosts[otherName];
            if (typeof(ref) === "undefined") {
                self.logger.info("Discovered a new proxy", { name: otherName });
                ref = new YtCoordinatedHost(self.config, otherName);
                self.hosts[otherName] = ref;

                ref.on("dead", function() {
                    self.logger.info("Marking proxy as dead", { name: otherName });
                });
                ref.on("alive", function() {
                    self.logger.info("Marking proxy as alive", { name: otherName });
                });
                ref.on("banned", function() {
                    self.logger.info("Proxy was banned", { name: otherName });
                });
                ref.on("unbanned", function() {
                    self.logger.info("Proxy was unbanned", { name: otherName });
                });
            }

            self.__DBG("Proxy '%s' has been updated to %j", otherName, entry);

            try {
                ref.role = utils.getYsonAttribute(entry, "role");
                ref.banned = utils.getYsonAttribute(entry, "banned");
                ref.ban_message = utils.getYsonAttribute(entry, "ban_message");
                ref.liveness = utils.getYsonAttribute(entry, "liveness");
            } catch (err) {
                var error = YtError.ensureWrapped(err);
                self.logger.error(
                    "Failed to update coordination information for '" + otherName + "'",
                    { error: error.toJson() });
            }
        });
    })
    .then(function() {
        deferred.resolve();
    })
    .catch(function(err) {
        var error = YtError.ensureWrapped(err);
        self.logger.error(
            "An error occurred while updating coordination",
            // TODO(sandello): Embed.
            { error: error.toJson() });

        deferred.reject(error);

        // Re-run initialization next time, just in case.
        self.initialized = false;
        self.initDeferred = Q.defer();
    })
    .done();
};

YtCoordinator.prototype.getProxies = function(role, dead, banned)
{
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
    if (!this.config.enable) {
        return true;
    }

    if (!this.initialized) {
        return false;
    }

    if ((new Date() - this.sync_at) > this.config.death_age) {
        return false;
    }

    if (this.host.banned) {
        return false;
    }

    return true;
};

YtCoordinator.prototype.getSelf = function()
{
    return this.host;
};

YtCoordinator.prototype.allocateProxy = function(role)
{
    var victims = this
        .getProxies(role, false, false)
        .sort(function(lhs, rhs) { return lhs.fitness - rhs.fitness; });

    if (victims.length > 0) {
        var victim = victims[0];
        if (typeof(victim) !== "undefined") {
            victim.dampening += 1;
        }
        return victim;
    }

    return null;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtCoordinator;
