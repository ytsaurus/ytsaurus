var buffertools = require("buffertools");
var url = require("url");
var querystring = require("querystring");
var fs = require("fs");
var mustache = require("mustache");
var Q = require("bluebird");

var YtAuthentication = require("./authentication.js").that;
var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("A", "Authentication");

var _TEMPLATE_INDEX = mustache.compile(fs.readFileSync(
    __dirname + "/../static/auth-index.mustache").toString());
var _TEMPLATE_LAYOUT = mustache.compile(fs.readFileSync(
    __dirname + "/../static/auth-layout.mustache").toString());
var _TEMPLATE_TOKEN = mustache.compile(fs.readFileSync(
    __dirname + "/../static/auth-token.mustache").toString());
var _TEMPLATE_ERROR = mustache.compile(fs.readFileSync(
    __dirname + "/../static/auth-error.mustache").toString());
var _STATIC_STYLE = fs.readFileSync(__dirname + "/../static/bootstrap.min.css");

////////////////////////////////////////////////////////////////////////////////

function YtApplicationAuth(config, logger, profiler, authority)
{
    this.config = config;
    this.logger = logger;
    this.profiler = profiler;
    this.authority = authority;
}

YtApplicationAuth.prototype.dispatch = function(req, rsp, next)
{
    var self = this;
    self.logger.debug("Auth call on '" + req.url + "'");

    return self._captureBody(req, rsp)
    .then(function(body) {
        switch (url.parse(req.url).pathname) {
            case "/":
            case "/index":
                return self._dispatchIndex(req, rsp, body);
            case "/login":
                return self._dispatchLogin(req, rsp, body);
            case "/whoami":
                return self._dispatchWhoAmI(req, rsp, body);
            case "/new":
                return self._dispatchNew(req, rsp, body);
            // There are only static routes below.
            case "/style":
                return utils.dispatchAs(rsp, _STATIC_STYLE, "text/css");
        }
        throw new YtError("Unknown URI");
    }).catch(self._dispatchError.bind(self, req, rsp));
};

YtApplicationAuth.prototype._dispatchError = function(req, rsp, err)
{
    var error = YtError.ensureWrapped(err);
    var logger = req.logger || this.logger;

    var message = error.message;
    var description;

    if (message) {
        logger.info("Error was caught in ApplicationAuth", {
            // TODO(sandello): Embed.
            error: error.toJson()
        });
    }

    if (req.url.indexOf("/login") === 0) {
        if (!error.isOK()) {
            rsp.statusCode = 400;
        }
        return utils.dispatchAs(rsp, error.toJson(), "application/json");
    } else {
        var sent_headers = !!rsp._header;
        if (sent_headers) {
            return void 0;
        }

        try {
            if (error.attributes.stack) {
                description = JSON.parse(error.attributes.stack);
            } else {
                description = JSON.stringify(JSON.parse(error.toJson()), null, 2);
            }
        } catch (err) {
        }

        var body = _TEMPLATE_LAYOUT({ content: _TEMPLATE_ERROR({
            message: message,
            description: description
        })});
        return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
    }
};

YtApplicationAuth.prototype._dispatchIndex = function(req, rsp)
{
    if (!utils.redirectUnlessDirectory(req, rsp)) {
        var body = _TEMPLATE_LAYOUT({ content: _TEMPLATE_INDEX() });
        return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
    }
};

YtApplicationAuth.prototype._dispatchLogin = function(req, rsp, body)
{
    var self = this;
    var logger = req.logger || self.logger;
    var profiler = self.profiler;
    var origin = req.origin || req.connection.remoteAddress;

    if (req.method !== "POST") {
        throw new YtError("Expected POST request");
    }

    if (typeof(body) !== "object" || typeof(body.token) !== "string") {
        throw new YtError("Expected body to have a `token` field");
    }

    return self.authority.authenticateByToken(logger, profiler, origin, body.token)
    .then(function(result) {
        return utils.dispatchJson(rsp, {
            login: result.login,
            realm: result.realm
        });
    })
    .catch(function(err) {
        return Q.reject(YtError.ensureWrapped(
            err, "Failed to authenticate by token"));
    });
};

YtApplicationAuth.prototype._dispatchWhoAmI = function(req, rsp)
{
    var deferred = Q.defer();
    var self = this;

    (new YtAuthentication(
        self.config,
        req.logger || self.logger,
        self.profiler,
        self.authority,
        false)).dispatch(
            req,
            rsp,
            function() {
                rsp.setHeader("Pragma", "nocache");
                rsp.setHeader("Expires", "Thu, 01 Jan 1970 00:00:01 GMT");
                rsp.setHeader("Cache-Control", "max-age=0, must-revalidate, proxy-revalidate, no-cache, no-store, private");
                rsp.setHeader("X-Content-Type-Options", "nosniff");
                rsp.setHeader("X-Frame-Options", "SAMEORIGIN");
                rsp.setHeader("X-DNS-Prefetch-Control", "off");

                utils.dispatchJson(rsp, {
                    login: req.authenticated_user,
                    realm: req.authenticated_from,
                    csrf_token: req.csrf_token,
                });
                deferred.resolve();
                return void 0;
            }, function() {
                deferred.reject();
                return void 0;
            });

    return deferred.promise;
};

YtApplicationAuth.prototype._dispatchNew = function(req, rsp)
{
    var params = querystring.parse(url.parse(req.url).query);
    if (params.code && params.state) {
        return this._dispatchNewCallback(req, rsp, params);
    } else {
        return this._dispatchNewRedirect(req, rsp, params);
    }
};

YtApplicationAuth.prototype._dispatchNewCallback = function(req, rsp, params)
{
    var self = this;
    var logger = req.logger || self.logger;
    var profiler = self.profiler;
    var origin = req.origin || req.connection.remoteAddress;

    var state = JSON.parse(params.state);

    return self.authority.oAuthObtainToken(
        logger,
        origin,
        state.realm,
        params.code)
    .then(function(token) {
        return Q.all([
            token,
            self.authority.authenticateByToken(logger, profiler, origin, token)
        ]);
    })
    .spread(function(token, result) {
        var hostname_pattern = "[a-zA-Z0-9.-]+\.yandex(-team\.ru|\.net)($|/)";
        var login = result.login;
        var realm = result.realm;
        if (state.return_path) {
            var target = state.return_path;
            target = url.parse(target);
            target.query = querystring.decode(target.query);
            target.query.token = token;
            target.query.login = login;
            target.search = null;
            target.path = null;

            if (target.hostname.match(hostname_pattern) == null) {
                throw new Error("Return path hostname is not safe");
            }
            if (!(target.protocol === "http:" || target.protocol === "https:")) {
                throw new Error("Return path protocol must be \"http\" or \"https\"");
            }

            target = url.format(target);
            return utils.redirectTo(rsp, target, 303);
        } else {
            var body = _TEMPLATE_LAYOUT({ content: _TEMPLATE_TOKEN({
                token: token,
                login: login,
                realm: realm,
            })});
            return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
        }
    })
    .catch(function(err) {
        return Q.reject(new YtError(
            "Failed to receive OAuth token or Blackbox login",
            err));
    });
};

YtApplicationAuth.prototype._dispatchNewRedirect = function(req, rsp, params)
{
    var logger = req.logger || this.logger;
    var origin = req.origin || req.connection.remoteAddress;

    var state = params;

    return this.authority.oAuthBuildUrlToRedirect(
        logger,
        origin,
        state.realm,
        state)
    .then(function(target) {
        return utils.redirectTo(rsp, target, 303);
    });
};

YtApplicationAuth.prototype._captureBody = function(req, rsp)
{
    var deferred = Q.defer();
    var chunks = [];

    req.on("data", function(chunk) { chunks.push(chunk); });
    req.on("end", function() {
        try {
            var body = buffertools.concat.apply(undefined, chunks);
            var result = querystring.parse(body.toString("utf-8"));
            deferred.resolve(result);
        } catch (err) {
            deferred.reject(err);
        }
    });

    return deferred.promise;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationAuth;
