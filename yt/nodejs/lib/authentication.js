var cookies = require("cookies");
var Q = require("bluebird");

var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

function YtAuthentication(config, logger, authority)
{
    "use strict";

    this.config = config;
    this.logger = logger;
    this.authority = authority;
}

YtAuthentication.prototype.dispatch = function(req, rsp, next)
{
    "use strict";

    var config = this.config;
    var logger = this.logger;
    var authority = this.authority;

    // This is an epilogue to call in case of successful authentication.
    function epilogue(login, realm) {
        logger.debug("Client has been authenticated", {
            authenticated_user: login,
            authenticated_from: realm
        });
        req.authenticated_user = login;
        req.authenticated_from = realm;
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
            return void utils.dispatchUnauthorized(rsp, "YT");
        }

        if (token) {
            result = authority.authenticateByToken(
                logger,
                req.origin || req.connection.remoteAddress,
                token);
        }
    } else {
        var jar = new cookies(req, rsp);
        var sessionid = jar.get("Session_id");
        var sslsessionid = jar.get("sessionid2");

        if (sessionid || sslsessionid) {
            result = authority.authenticateByCookie(
                logger,
                req.origin || req.connection.remoteAddress,
                sessionid,
                sslsessionid);
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
            return void epilogue(result.login, result.realm);
        } else {
            logger.debug("Client has failed to authenticate");
            return void utils.dispatchUnauthorized(rsp, "YT");
        }
    },
    function(err) {
        var error = YtError.ensureWrapped(err);
        // XXX(sandello): Embed.
        logger.info("An error occured during authentication", {
            error: error.toJson()
        });
        return void utils.dispatchLater(rsp, 60);
    })
    .done();
};


////////////////////////////////////////////////////////////////////////////////

exports.that = YtAuthentication;
