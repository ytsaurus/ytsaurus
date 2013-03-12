var url = require("url");
var qs = require("querystring");
var fs = require("fs");
var lru_cache = require("lru-cache");
var connect = require("connect");
var mustache = require("mustache");
var http = require("http");
var Q = require("q");

var YtRegistry = require("./registry").that;
var YtError = require("./error").that;
var YtHttpClient = require("./http_client").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var TEMPLATE_INDEX = mustache.compile(fs.readFileSync(
    __dirname + "/../static/auth-index.mustache").toString());
var TEMPLATE_LAYOUT = mustache.compile(fs.readFileSync(
    __dirname + "/../static/auth-layout.mustache").toString());
var TEMPLATE_TOKEN = mustache.compile(fs.readFileSync(
    __dirname + "/../static/auth-token.mustache").toString());
var STATIC_STYLE = fs.readFileSync(__dirname + "/../static/bootstrap.min.css");

function YtApplicationAuth()
{
    var logger = YtRegistry.get("logger");
    var config = YtRegistry.get("config");

    function requestOAuthToken(key, code)
    {
        var app_config = YtRegistry.query(
            "config",
            ".oauth.applications{.key === $key}",
            { key: key });

        if (app_config.length !== 1) {
            return Q.reject(new YtError(
                "There is no application with key " + JSON.stringify(key) + "."));
        } else {
            app_config = app_config[0]; // Unbox.
        }

        return YtHttpClient(
            config.oauth.host,
            config.oauth.port,
            config.oauth.token_path,
            "POST")
        .withBody(qs.stringify({
            code : code,
            grant_type : "authorization_code",
            client_id : app_config.client_id,
            client_secret : app_config.client_secret
        }), "application/www-form-urlencoded")
        .setNoDelay(true)
        .setTimeout(config.oauth.timeout)
        .then(function(data) {
            try {
                var error;
                var result = JSON.parse(data);
                if (result.access_token) {
                    return data.access_token;
                } else if (result.error) {
                    error = new YtError(
                        "OAuth server returned an error: " + result.error);
                    error.attributes.raw_data = data;
                    throw error;
                } else {
                    error = new YtError(
                        "OAuth server returned a malformed result");
                    error.attributes.raw_data = data;
                    throw error;
                }
            } catch(err) {
                throw new YtError("OAuth server returned an invalid JSON", err);
            }
        });
    }

    function handleIndex(req, rsp)
    {
        if (!utils.redirectUnlessDirectory(req, rsp)) {
            var body = TEMPLATE_LAYOUT({ content: TEMPLATE_INDEX() });
            utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
        }
    }

    function handleNew(req, rsp)
    {
        var params = qs.parse(url.parse(req.url).query);

        if (params.code && params.state) {
            try {
                var state = JSON.parse(params.state);
            } catch (err) {
                rsp.statusCode = 500;
                rsp.end();
            }

            logger.debug("Requesting OAuth token", {
                request_id : req.uuid,
                state : state
            });

            Q.when(requestOAuthToken(state.key, params.code),
            function(token) {
                logger.debug("Successfully received OAuth token", {
                    request_id : req.uuid
                });

                if (state.return_path) {
                    var target = state.return_path;
                    target = url.parse(target);
                    target.query = qs.parse(target.query);
                    target.query.token = token;
                    target.query = qs.format(target.query);
                    target = url.format(target);
                    return utils.redirectTo(rsp, target, 303);
                } else {
                    var body = TEMPLATE_LAYOUT({ content: TEMPLATE_TOKEN({
                        token : token
                    })});
                    return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
                }
            },
            function(error) {
                logger.debug("Failed to receive OAuth token", {
                    request_id : req.uuid,
                    error : error.toString()
                    // XXX(sandello): Better embedding would be nice.
                });
                var body = TEMPLATE_LAYOUT({ content: TEMPLATE_TOKEN({
                    error : error
                })});
                return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
            });
        } else {
            var app_config = YtRegistry.query(
                "config",
                ".oauth.applications{.key === $key}",
                { key: params.application || "api" });

            if (app_config.length !== 1) {
                var body = TEMPLATE_LAYOUT({ content: TEMPLATE_TOKEN({
                    error : new Error("Unknown application.")
                })});
                return utils.dispatchAs(rsp, body, "text/html; charset=utf-8");
            } else {
                app_config = app_config[0]; // Unbox.
            }

            var target = url.format({
                protocol : "http",
                host : config.oauth.host,
                port : config.oauth.port,
                pathname : config.oauth.authorize_path,
                query : {
                    response_type : "code",
                    display : "popup",
                    client_id : app_config.client_id,
                    state : JSON.stringify({
                        key : app_config.key,
                        return_path : params.return_path
                    })
                }
            });

            return utils.redirectTo(rsp, target, 303);
        }
    }

    return function(req, rsp) {
        switch (url.parse(req.url).pathname) {
            case "/":
            case "/index":
                return handleIndex(req, rsp);
            case "/new":
                return handleNew(req, rsp);
            // There are only static routes below.
            case "/style":
                return utils.dispatchAs(rsp, STATIC_STYLE, "text/css");
        }
    };
};

exports.that = YtApplicationAuth;
