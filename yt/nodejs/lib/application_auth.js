var url = require("url");
var qs = require("querystring");
var fs = require("fs");
var mustache = require("mustache");
var Q = require("q");

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
var _STATIC_STYLE = fs.readFileSync(__dirname + "/../static/bootstrap.min.css");

////////////////////////////////////////////////////////////////////////////////

function YtApplicationAuth(config, logger, authority)
{
    "use strict";

    this.config = config;
    this.logger = logger;
    this.driver = driver;

    this.authority = authority;
}

YtApplicationAuth.prototype.dispatch = function(req, rsp, next)
{
    "use strict";

    try {
        switch (url.parse(req.url).pathname) {
            case "/":
            case "/index":
                return this._dispatchIndex(req, rsp);
            case "/new":
                return this._dispatchNew(req, rsp);
            // There are only static routes below.
            case "/style":
                return utils.dispatchAs(rsp, _STATIC_STYLE, "text/css");
        }
        throw new YtError("Unknown URI");
    } catch(err) {
        return this._dispatchError(YtError.ensureWrapped(err));
    }
};

YtApplicationAuth.prototype._dispatchError = function(req, rsp, error)
{
    "use strict";

    var body = _TEMPLATE_LAYOUT({ content: _TEMPLATE_TOKEN({
        error: error.message
    })});
    return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
};

YtApplicationAuth.prototype._dispatchIndex = function(req, rsp)
{
    "use strict";

    if (!utils.redirectUnlessDirectory(req, rsp)) {
        var body = _TEMPLATE_LAYOUT({ content: _TEMPLATE_INDEX() });
        return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
    }
};

YtApplicationAuth.prototype._dispatchNew = function(req, rsp)
{
    "use strict";

    var params = qs.parse(url.parse(req.url).query);
    if (params.code && params.state) {
        return this._dispatchNewCallback(req, rsp, params);
    } else {
        return this._dispatchNewRedirect(req, rsp, params);
    }
};

YtApplicationAuth.prototype._dispatchNewCallback = function(req, rsp, params)
{
    "use strict";

    var self = this;
    var state = JSON.parse(params.state);

    Q
    .when(self.authority.oAuthObtainToken(
        self.logger,
        req.connection.remoteAddress,
        state.realm,
        params.code))
    .then(function(token) {
        return Q.all([
            token,
            self.authority.authenticate(
                self.logger,
                req.connection.remoteAddress,
                token)
        ]);
    })
    .spread(function(token, result) {
        return Q
        .when(self._ensureUser(result.login))
        .then(function() { return [ token, result.login, result.realm ]; });
    })
    .spread(function(token, login, realm) {
        if (state.return_path) {
            var target = state.return_path;
            // TODO(sandello): Fixme.
            /*
            target = url.parse(target);
            target.query = qs.decode(target.query);
            target.query.token = token;
            target.query.login = login;
            target.query = qs.encode(target.query);
            target = url.format(target);
            */
            target += "?token=" + token + "&login=" + login;
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
    .fail(function(err) {
        var error = new YtError(
            "Failed to receive OAuth token or Blackbox login",
            err);
        // XXX(sandello): Embed.
        self.logger.info(error.message, { error: error.toJson() });
        return self._dispatchError(req, rsp, error);
    })
    .end();
};

YtApplicationAuth.prototype._dispatchNewRedirect = function(req, rsp, params)
{
    "use strict";

    var state = params;
    var target = this.authority.oAuthUrlToRedirect(
        this.logger,
        req.connection.remoteAddress,
        state.realm,
        state);
    return utils.redirectTo(rsp, target, 303);
};

YtApplicationAuth.prototype._ensureUser = function(name)
{
    "use strict";

    return Q
    .when(name)
    .then(function(name) {
        var path = "//sys/users/" + utils.escapeYPath(name);
        return this.driver.executeSimple("exists", { path: path });
    })
    .then(function(exists) {
        if (exists === "true") {
            return;
        }
        return this.driver.executeSimple("create", {
            type: "user",
            attributes: { name: name }
        });
    })
    .then(function(create) {
        return;
    });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationAuth;
