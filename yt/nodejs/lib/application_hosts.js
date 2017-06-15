var url = require("url");
var qs = require("qs");

var Q = require("bluebird");

var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("H", "Hosts");

function formatHostWithPattern(pattern, name)
{
    var fragments = name.split(".");
    var host = fragments[0];
    var domain = fragments.slice(1).join(".");

    return pattern
        .replace("HOST", host)
        .replace("DOMAIN", domain);
}

////////////////////////////////////////////////////////////////////////////////

function YtApplicationHosts(logger, coordinator, show_ports, rewrite_yandex_team_domain, hosts)
{
    this.logger = logger;
    this.coordinator = coordinator;
    this.show_ports = show_ports;
    this.rewrite_yandex_team_domain = rewrite_yandex_team_domain;
    this.hosts = hosts;
}

YtApplicationHosts.prototype.dispatch = function(req, rsp, next)
{
    var self = this;
    self.logger.debug("Hosts call on '" + req.url + "'");

    return Q.try(function() {
        var parsed_url = url.parse(req.url);
        var parsed_query = qs.parse(parsed_url.query);
        var suffix = parsed_url.pathname;
        suffix = suffix.replace(/\/+$/, "").substr(1);
        var role = parsed_query.role;
        role = typeof(role) !== "undefined" ? role : "data";
        if (suffix === "all") {
            return self._dispatchExtended(req, rsp);
        } else {
            return self._dispatchBasic(req, rsp, suffix, role);
        }
    }).catch(self._dispatchError.bind(self, req, rsp));
};

YtApplicationHosts.prototype._dispatchError = function(req, rsp, err)
{
    var error = YtError.ensureWrapped(err);
    return utils.dispatchAs(rsp, error.toJson(), "application/json");
};

YtApplicationHosts.prototype._dispatchBasic = function(req, rsp, suffixName, role)
{
    var formatHost;
    if (this.hosts.hasOwnProperty(suffixName)) {
        formatHost = formatHostWithPattern.bind(this, this.hosts[suffixName]);
    } else if (!suffixName) {
        formatHost = function (name) {
            return name;
        };
    } else {
        rsp.statusCode = 404;
        throw new YtError("Unknown URI");
    }

    var hosts = this.coordinator
    .getProxies(role, false, false)
    .sort(function(lhs, rhs) { return lhs.fitness - rhs.fitness; })
    .map(function(entry) { return formatHost(entry.name); });

    if (this.rewrite_yandex_team_domain) {
        var need_to_rewrite = false;

        var origin = req.headers.origin;
        if (typeof(origin) === "string") {
            if (/yandex-team\.ru$/.test(origin)) {
                need_to_rewrite = true;
            }
        }
        var host = req.headers.host;
        if (typeof(host) === "string") {
            if (/yandex-team\.ru/.test(host)) {
                need_to_rewrite = true;
            }
        }

        if (need_to_rewrite) {
            hosts = hosts.map(function(entry) {
                return entry.replace("yandex.net", "yandex-team.ru");
            });
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

    this.coordinator.allocateProxy(role);

    return utils.dispatchAs(rsp, body, mime);
};

YtApplicationHosts.prototype._dispatchExtended = function(req, rsp)
{
    var data = this.coordinator.getProxies();
    return utils.dispatchJson(rsp, data);
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationHosts;
