var Q = require("q");

var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("A", "Authentication");

////////////////////////////////////////////////////////////////////////////////

function YtAuthentication(config, logger, authority)
{
    "use strict";
    this.__DBG = __DBG.Tagged();

    this.config = config;
    this.logger = logger;
    this.authority = authority;

    this.token = undefined;
    this.login = false;
    this.realm = false;

    this.__DBG("New");
}

YtAuthentication.prototype.dispatch = function(req, rsp, next)
{
    "use strict";
    this.__DBG("dispatch");

    if (this._extractToken(req, rsp)) {
        return this._epilogue(req, rsp, next);
    }

    if (this._rejectBadClients(req, rsp)) {
        return this._epilogue(req, rsp, next);
    }

    var result = this.authority.authenticate(
        this.logger,
        req.connection.remoteAddress,
        this.token);

    if (Q.isPromise(result)) {
        this.login = result.get("login");
        this.realm = result.get("realm");
    } else {
        this.login = result.login;
        this.realm = result.realm;
    }

    return this._epilogue(req, rsp, next);
};

YtAuthentication.prototype._epilogue = function(req, rsp, next)
{
    "use strict";
    this.__DBG("_epilogue");

    var self = this;

    Q
    .all([self.login, self.realm])
    .spread(
    function(login, realm) {
        if (typeof(login) === "string" && typeof(realm) === "string") {
            self.logger.debug("Client has been authenticated", {
                authenticated_user: login,
                authenticated_from: realm
            });
            req.authenticated_user = login;
            req.authenticated_from = realm;
            process.nextTick(next);
            return;
        } else {
            self.logger.debug("Client has failed to authenticate");
            return utils.dispatchUnauthorized(
                rsp,
                "OAuth scope=" + JSON.stringify(self.config.grant));
        }
    },
    function(err) {
        var error = YtError.ensureWrapped(err);
        // XXX(sandello): Embed.
        self.logger.info(error.message, { error: error.toJson() });
        return utils.dispatchLater(rsp, 60);
    })
    .end();
};

YtAuthentication.prototype._extractToken = function(req, rsp)
{
    "use strict";
    this.__DBG("_extractToken");

    if (!req.headers.hasOwnProperty("authorization")) {
        this.logger.debug("Client is missing Authorization header");
        // Presumably allow guest access.
        this.login = this.config.guest_login;
        this.realm = this.config.guest_realm;
        return false;
    }

    var parts = req.headers["authorization"].split(/\s+/);
    var token = parts[1];

    if (parts[0] !== "OAuth" || !token) {
        this.logger.debug("Client has improper Authorization header", {
            header: req.headers["authorization"]
        });
        // Reject all invalid requests.
        return true;
    }

    this.token = token;
};

YtAuthentication.prototype._rejectBadClients = function(req, rsp)
{
    "use strict";
    this.__DBG("_rejectBadClients");

    if (typeof(this.token) === "undefined") {
        var ua = req.headers["user-agent"];
        if (ua && (
            ua.indexOf("Python wrapper") === 0 ||
            ua.indexOf("C++ wrapper") === 0))
        {
            this.logger.debug(
                "Client is required to provide Authorization token");
            // Revoke guest access for Python and C++ clients.
            this.login = false;
            this.realm = false;
        }
        // If there is no token up to the moment then fail quickly.
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtAuthentication;
