var cookies = require("cookies");
var Q = require("bluebird");

var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

function YtAuthentication(config, logger, profiler, authority, check_csrf_token)
{
    "use strict";

    this.config = config;
    this.logger = logger;
    this.profiler = profiler;
    this.authority = authority;
    this.check_csrf_token = check_csrf_token;
}

YtAuthentication.prototype.dispatch = function(req, rsp, next, prev)
{
    "use strict";

    var config = this.config;
    var logger = this.logger;
    var profiler = this.profiler;
    var authority = this.authority;

    // This is an epilogue to call in case of successful authentication.
    function epilogue(login, realm, blackbox_userid) {
        logger.debug("Client has been authenticated", {
            authenticated_user: login,
            authenticated_from: realm
        });
        req.authenticated_user = login;
        req.authenticated_from = realm;

        if (blackbox_userid) {
            req.csrf_token = authority.signCsrfToken(blackbox_userid, + new Date());
        }
        process.nextTick(next);
    }

    // Fail fast if authentication is disabled.
    if (!config.enable) {
        logger.debug("Authentication is disabled");
        // Fallback to root credentials.
        return void epilogue("root", "root");
    }

    var result = null;

    if (req.headers.hasOwnProperty("authorization")) {
        var parts = req.headers["authorization"].split(/\s+/);
        var token = parts[1];

        if (parts[0] !== "OAuth" || !token) {
            logger.debug("Client has improper 'Authorization' header", {
                header: req.headers["authorization"]
            });
            // Reject all invalid requests.
            utils.dispatchUnauthorized(rsp, "YT", "Invalid 'Authorization' header");
            if (typeof(prev) === "function") { prev(); }
            return void 0;
        }

        if (token) {
            result = authority.authenticateByToken(
                logger,
                profiler,
                req.origin || req.connection.remoteAddress,
                token);
        }
    } else {
        var jar = new cookies(req, rsp);
        var sessionid = jar.get("Session_id");
        var sslsessionid = jar.get("sessionid2");
        var csrf_token = req.headers["x-csrf-token"];

        if (sessionid || sslsessionid) {
            result = authority.authenticateByCookie(
                logger,
                profiler,
                req.origin || req.connection.remoteAddress,
                sessionid,
                sslsessionid,
                csrf_token,
                this.check_csrf_token);
        }
    }

    if (!result) {
        logger.debug("Client is missing credentials");
        // Fallback to guest credentials.
        return void epilogue(config.guest_login, config.guest_realm);
    }

    return void result.then(
    function(result) {
        if (result.isAuthenticated) {
            return void epilogue(result.login, result.realm, result.blackbox_userid);
        } else {
            logger.debug("Client has failed to authenticate");
            utils.dispatchUnauthorized(rsp, "YT", "Authentication has failed");
            if (typeof(prev) === "function") { prev(); }
            return void 0;
        }
    },
    function(err) {
        var error = YtError.ensureWrapped(err);
        // XXX(sandello): Embed.
        logger.info("An error occurred during authentication", {
            error: error.toJson()
        });
        utils.dispatchLater(rsp, 60, "Authentication is currently unavailable");
        if (typeof(prev) === "function") { prev(); }
        return void 0;
    })
    .done();
};


////////////////////////////////////////////////////////////////////////////////

exports.that = YtAuthentication;
