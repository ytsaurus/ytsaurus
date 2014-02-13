var url = require("url");

var Q = require("q");

var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("H", "Hosts");

function addHostNameSuffix(host, suffix)
{
    var index = host.indexOf(".");
    if (index > 0) {
        return host.substr(0, index) + suffix + host.substr(index);
    } else {
        return host + suffix;
    }
}

////////////////////////////////////////////////////////////////////////////////

function YtApplicationHosts(logger, coordinator)
{
    "use strict";

    this.logger = logger;
    this.coordinator = coordinator;
}

YtApplicationHosts.prototype.dispatch = function(req, rsp, next)
{
    "use strict";

    var self = this;
    self.logger.debug("Hosts call on '" + req.url + "'");

    return Q.try(function() {
        var suffix;
        suffix = url.parse(req.url).pathname;
        suffix = suffix.replace(/\/+/, "-").replace(/-+$/, "")
        if (suffix === "-all") {
            return self._dispatchExtended(req, rsp);
        } else {
            return self._dispatchBasic(req, rsp, suffix);
        }
        throw new YtError("Unknown URI");
    })
    .fail(self._dispatchError.bind(self, req, rsp));
};

YtApplicationHosts.prototype._dispatchError = function(req, rsp, err)
{
    "use strict";
    var error = YtError.ensureWrapped(err);
    return utils.dispatchAs(rsp, error.toJson(), "application/json");
};

YtApplicationHosts.prototype._dispatchBasic = function(req, rsp, suffix)
{
    "use strict";

    var hosts = this.coordinator
    .getProxies("data", false, false)
    .sort(function(lhs, rhs) { return lhs.fitness - rhs.fitness; })
    .map(function(entry) { return addHostNameSuffix(entry.host, suffix); });

    var mime, body;
    mime = utils.bestAcceptedType(
        [ "application/json", "text/plain" ],
        req.headers["accept"]);
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
    "use strict";

    var data = this.coordinator.getProxies();
    return utils.dispatchJson(rsp, data);
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationHosts;
