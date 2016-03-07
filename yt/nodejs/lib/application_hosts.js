var url = require("url");

var Q = require("bluebird");

var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("H", "Hosts");

function addHostNameSuffix(name, suffix)
{
    var index = name.indexOf(".");
    if (index > 0) {
        return name.substr(0, index) + suffix + name.substr(index);
    } else {
        return name + suffix;
    }
}

////////////////////////////////////////////////////////////////////////////////

function YtApplicationHosts(logger, coordinator, show_ports, rewrite_yandex_team_domain)
{
    this.logger = logger;
    this.coordinator = coordinator;
    this.show_ports = show_ports;
    this.rewrite_yandex_team_domain = rewrite_yandex_team_domain;
}

YtApplicationHosts.prototype.dispatch = function(req, rsp, next)
{
    var self = this;
    self.logger.debug("Hosts call on '" + req.url + "'");

    return Q.try(function() {
        var suffix;
        suffix = url.parse(req.url).pathname;
        suffix = suffix.replace(/\/+/, "-").replace(/-+$/, "");
        if (suffix === "-all") {
            return self._dispatchExtended(req, rsp);
        } else {
            return self._dispatchBasic(req, rsp, suffix);
        }
        throw new YtError("Unknown URI");
    }).catch(self._dispatchError.bind(self, req, rsp));
};

YtApplicationHosts.prototype._dispatchError = function(req, rsp, err)
{
    var error = YtError.ensureWrapped(err);
    return utils.dispatchAs(rsp, error.toJson(), "application/json");
};

YtApplicationHosts.prototype._dispatchBasic = function(req, rsp, suffix)
{
    var hosts = this.coordinator
    .getProxies("data", false, false)
    .sort(function(lhs, rhs) { return lhs.fitness - rhs.fitness; })
    .map(function(entry) { return addHostNameSuffix(entry.name, suffix); });

    if (this.rewrite_yandex_team_domain) {
        var origin = req.headers.origin;
        if (typeof(origin) === "string") {
            try {
                origin = url.parse(origin).hostname;
                if (/\yandex-team\.ru$/.test(origin)) {
                    hosts = hosts.map(function(entry) {
                        return entry.replace("yandex.net", "yandex-team.ru");
                    });
                }
            } catch (ex) {
            }
        }
    }

    if (!this.show_ports) {
        hosts = hosts.map(function(entry) {
            return entry.split(":")[0];
        });
    }

    var accept = req.headers.accept;
    var mime, body;
    mime = utils.bestAcceptedType(["application/json", "text/plain"], accept);
    mime = mime || "application/json";

    switch (mime) {
        case "application/json":
            body = JSON.stringify(hosts);
            break;
        case "text/plain":
            body = hosts.join("\n");
            break;
    }

    this.coordinator.allocateDataProxy();

    return utils.dispatchAs(rsp, body, mime);
};

YtApplicationHosts.prototype._dispatchExtended = function(req, rsp)
{
    var data = this.coordinator.getProxies();
    return utils.dispatchJson(rsp, data);
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationHosts;
