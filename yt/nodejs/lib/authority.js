var lru_cache = require("lru-cache");
var Q = require("q");

var YtError = require("./error").that;
var YtRegistry = require("./registry").that;

var external_services = require("./external_services");
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("A", "Authentication");

////////////////////////////////////////////////////////////////////////////////

function YtAuthority(config, driver)
{
    "use strict";
    this.__DBG = __DBG.Tagged();

    this.config = config;
    this.driver = driver;

    // Caches token authentication results.
    this.token_cache = lru_cache({
        max: this.config.cache_max_size,
        maxAge: this.config.cache_max_token_age,
    });

    // Caches user existence.
    this.exist_cache = lru_cache({
        max: this.config.cache_max_size,
        maxAge: this.config.cache_max_exist_age,
    });

    this.__DBG("New");
}

YtAuthority.prototype.authenticate = function(logger, party, token)
{
    "use strict";
    this.__DBG("authenticate");

    // This structure represents an immutable request state.
    // It is passed to all subsequent calls.
    var context = {
        ts: new Date(),
        logger: logger,
        party: party,
        token: token,
    };

    // This structure represents a mutable response state.
    // If |result.login| is string, then it is a login.
    // If |result.login| === false, then it is a rejection.
    var result = {
        login: false,
        realm: false,
    };

    Object.defineProperty(result, "isAuthenticated", {
        get: function() {
            return typeof(this.login) === "string"
                && typeof(this.realm) === "string";
        },
        enumerable: true
    });

    // Reject empty tokens.
    if (token === "") {
        result.realm = "empty";
        return result;
    }

    // This is a fast function so we are not using the promise chain here.
    // We want to behave as fast as possible for these cases. So no fancy shit.
    if (this._syncCheckCache(context, result)) {
        return result;
    }

    // Cache |token_cache| variable. :)
    var token_cache = this.token_cache;

    // Perform proper authentication here and cache the result.
    return Q
    .when(this._asyncQueryCypress(context, result))
    .then(this._asyncQueryBlackbox.bind(this, context, result))
    .then(this._ensureUserExists.bind(this, context, result))
    .then(function() {
        var dt = (new Date()) - context.ts;
        if (result.isAuthenticated) {
            context.logger.debug("Authentication succeeded", {
                authentication_time: dt,
            });
            token_cache.set(context.token, result);
        } else {
            context.logger.info("Authentication failed", {
                authentication_time: dt,
            });
            token_cache.del(context.token);
        }
        return result;
    });
};

YtAuthority.prototype.oAuthObtainToken = function(
    logger, party, key, code)
{
    "use strict";
    this.__DBG("oAuthObtainToken");

    var app = this._findOAuthApplicationBy("key", key || this.config.default_oauth_application_key);
    if (typeof(app) === "undefined") {
        var error = new YtError(
            "There is no OAuth application with key " +
            JSON.stringify(key) + ".");
        return Q.reject(error);
    }

    return external_services.oAuthObtainToken(
        logger,
        app.client_id,
        app.client_secret,
        code)
    .then(function(data) {
        if (typeof(data.access_token) === "undefined") {
            return Q.reject(new YtError("Unreachable (you are lucky)"));
        }

        return data.access_token;
    });
};

YtAuthority.prototype.oAuthBuildUrlToRedirect = function(
    logger, party, key, state)
{
    "use strict";
    this.__DBG("oAuthBuildUrlToRedirect");

    var app = this._findOAuthApplicationBy("key", key || this.config.default_oauth_application_key);
    if (typeof(app) === "undefined") {
        var error = new YtError(
            "There is no OAuth application with key " +
            JSON.stringify(key) + ".");
        throw error;
    }

    return external_services.oAuthBuildUrlToRedirect(app.client_id, state);
};

YtAuthority.prototype._findOAuthApplicationBy = function(key, value)
{
    "use strict";
    this.__DBG("_findOAuthApplicationBy");

    var apps = this.config.oauth;
    for (var i = 0, n = apps.length; i < n; ++i) {
        if (apps[i][key] === value) {
            return apps[i];
        }
    }
};

YtAuthority.prototype._syncCheckCache = function(context, result)
{
    "use strict";
    this.__DBG("_syncCheckCache");

    var cached_result = this.token_cache.get(context.token);
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

YtAuthority.prototype._asyncQueryBlackbox = function(context, result)
{
    "use strict";
    this.__DBG("_asyncQueryBlackbox");

    var self = this;

    if (!self.config.blackbox.enable || result.isAuthenticated) {
        return;
    }

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
        var grant = self.config.blackbox.grant;
        if (scope.indexOf(grant) === -1) {
            context.logger.debug(
                "Token does not provide '" + grant + "' grant");
            return;
        }

        var realm = self._findOAuthApplicationBy("client_id", data.oauth.client_id);
        if (typeof(realm) === "undefined") {
            context.logger.debug("Token was issued by the unknown realm");
            return;
        } else {
            realm = realm.key;
        }

        var login = data.login;

        context.logger.debug("Blackbox has approved the token");

        result.login = login;
        result.realm = "blackbox-" + realm;
    });
};

YtAuthority.prototype._asyncQueryCypress = function(context, result)
{
    "use strict";
    this.__DBG("_asyncQueryCypress");

    var self = this;

    if (!self.config.cypress.enable || result.isAuthenticated) {
        return;
    }

    var path = self.config.cypress.where + "/" + utils.escapeYPath(context.token);

    return Q
    .when(self.driver.executeSimple("get", { path: path }))
    .then(
    function(login) {
        if (typeof(login) !== "string") {
            context.logger.debug("Encountered garbage at path '" + path + "'");
            return;
        }

        context.logger.debug("Cypress has approved the token");

        result.login = login;
        result.realm = "cypress";
    },
    function(error) {
        if (error.code === 500) {
            return; // Resolve error, return 'undefined';
        } else {
            return Q.reject(error);
        }
    });
};

YtAuthority.prototype._ensureUserExists = function(context, result)
{
    "use strict";
    this.__DBG("_ensureUserExists");

    var self = this;
    var name = result.login;

    if (!result.isAuthenticated || self.exist_cache.get(name)) {
        return;
    }

    return Q
    .when(self.driver.executeSimple("create", {
        type: "user",
        attributes: { name: name }
    }))
    .then(
    function(create) {
        context.logger.debug("User created", { name: name });
        self.exist_cache.set(name, true);
    },
    function(error) {
        if (error.code === 501) {
            context.logger.debug("User already exists", { name: name });
            self.exist_cache.set(name, true);
            return;
        } else {
            return Q.reject(error);
        }
    });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtAuthority;
