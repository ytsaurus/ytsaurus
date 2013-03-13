var lru_cache = require("lru-cache");
var Q = require("q");

var YtError = require("./error").that;
var YtRegistry = require("./registry").that;

var external_services = require("./external_services");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("A", "Authentication");

////////////////////////////////////////////////////////////////////////////////

function YtAuthority(config)
{
    "use strict";
    this.__DBG = __DBG.Tagged();

    this.config = config;
    this.cache  = lru_cache({
        max: this.config.cache_max_size,
        maxAge: this.config.cache_max_age,
    });

    this.__DBG("New");
}

YtAuthority.prototype.authenticate = function(logger, party, token)
{
    "use strict";
    this.__DBG("authenticate");

    // Fail fast. And in branch prediction we trust.
    if (!this.config.enable) {
        return { login: false, realm: false };
    }

    // This structure represents a request state and it is passed
    // to all subsequent calls. This structure shall be immutable.
    var context = {
        ts: new Date(),
        logger: logger,
        party: party,
        token: token,
    };

    // This structure represents a response state.
    // If |result.login| is string, then it is a login.
    // If |result.login| === false, then it is a rejection.
    var result = {
        login: false,
        realm: false,
    };

    // These are very fast functions so we are not using the promise chain here.
    // We want to behave as fast as possible for these cases. So no fancy shit.
    if (this._syncCheckCache(context, result)) {
        return result;
    }
    if (this._syncCheckLocal(context, result)) {
        return result;
    }

    // Cache |cache| variable. :)
    var cache = this.cache;

    // Perform proper authentication here and cache the result.
    return Q
    .when(this._asyncQueryBlackbox(context, result))
    .then(
    function() {
        var dt = (new Date()) - context.ts;
        context.logger.debug("Authentication succeeded", {
            login: result.login,
            realm: result.realm,
            authentication_time: dt,
        });
        cache.set(context.token, result);
        return result;
    },
    function(err) {
        var dt = (new Date()) - context.ts;
        var error = new YtError("Authentication failed", err);
        context.logger.info(error.message, {
            login: result.login,
            realm: result.realm,
            authentication_time: dt,
            // XXX(sandello): Embed.
            error: error.toJson()
        });
        cache.del(context.token);
        return Q.reject(error);
    });
};

YtAuthority.prototype.oAuthObtainToken = function(
    logger, party, realm_key, code)
{
    "use strict";
    this.__DBG("oAuthObtainToken");

    var realm = this._findRealmBy("key", realm_key, "oauth");
    if (typeof(realm) === "undefined") {
        var error = new YtError(
            "There is no OAuth realm with key " +
            JSON.stringify(realm_key) + ".");
        return Q.reject(error);
    }

    var self = this;

    return external_services.oAuthObtainToken(
        logger,
        realm.client_id,
        realm.client_secret,
        code)
    .then(function(data) {
        if (typeof(data.access_token) === "undefined") {
            return Q.reject(new YtError("Unreachable (you are lucky)"));
        }

        return data.access_token;
    });
};

YtAuthority.prototype.oAuthBuildUrlToRedirect = function(
    logger, party, realm_key, state)
{
    "use strict";
    this.__DBG("oAuthBuildUrlToRedirect");

    var realm = this._findRealmBy("key", realm_key, "oauth");
    if (typeof(realm) === "undefined") {
        var error = new YtError(
            "There is no OAuth realm with key " +
            JSON.stringify(realm_key) + ".");
        logger.info(error.message);
        throw error;
    }

    return external_services.oAuthBuildUrlToRedirect(realm.client_id, state);
};

YtAuthority.prototype._findRealmBy = function(key, value, type)
{
    "use strict";
    this.__DBG("_findRealmBy");
    var realms = this.config.realms;
    for (var i = 0, n = realms.length; i < n; ++i) {
        if (realms[i][key] === value) {
            if (typeof(type) === "undefined" || realms[i].type === type) {
                return realms[i];
            }
        }
    }
};

YtAuthority.prototype._syncCheckCache = function(context, result)
{
    "use strict";
    this.__DBG("_syncCheckCache");

    var cached_result = this.cache.get(context.token);
    if (typeof(cached_result) !== "undefined") {
        // Since |result| is a "pointer", we have to set fields explicitly.
        // We can't do something like |*result = cached_result;|.
        context.logger.debug("Authentication cache hit", {
            login: cached_result.login,
            realm: cached_result.realm
        });
        result.login = cached_result.login;
        result.realm = cached_result.realm;
        return true;
    } else {
        context.logger.debug("Authentication cache miss");
    }
};

YtAuthority.prototype._syncCheckLocal = function(context, result)
{
    "use strict";
    this.__DBG("_syncCheckLocal");

    var realm = this._findRealmBy("key", "local");
    if (typeof(realm) === "undefined" || realm.type !== "local") {
        return;
    }

    var token = context.token;
    var login = realm.tokens[token];
    if (typeof(login) === "string") {
        context.logger.debug("Token is local");
        result.login = login;
        result.realm = realm.key;
        return true;
    }
};

YtAuthority.prototype._asyncQueryBlackbox = function(context, result)
{
    "use strict";
    this.__DBG("_asyncQueryBlackbox");

    var self = this;

    return external_services.blackboxValidateToken(
        context.logger,
        context.party,
        context.token)
    .then(function(data) {
        // Since we are caching results, we don't have to bother too much about
        // the most efficient order here. We prefer to keep logic readable.
        if (typeof(data.error) === "undefined") {
            return Q.reject(new YtError("Unreachable (you are lucky)"));
        }

        if (!(data.error === "OK" &&
            typeof(data.oauth) === "object" &&
            typeof(data.login) === "string"))
        {
            context.logger.debug("Blackbox has rejected the token");
            return;
        }

        var scope = data.oauth.scope.split(/\s+/);
        var grant = self.config.grant;
        if (scope.indexOf(grant) === -1) {
            context.logger.debug(
                "Token does not provide '" + grant + "' grant");
            return;
        }

        var realm = self._findRealmBy("client_id", data.oauth.client_id);
        if (typeof(realm) === "undefined") {
            context.logger.debug("Token was issued by the unknown realm");
            return;
        } else {
            realm = realm.key;
        }

        var login = data.login;

        context.logger.debug("Blackbox has approved the token");

        result.login = login;
        result.realm = realm;
    });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtAuthority;
