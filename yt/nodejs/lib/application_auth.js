var url = require("url");
var qs = require("querystring");
var fs = require("fs");
var lru_cache = require("lru-cache");
var connect = require("connect");
var mustache = require("mustache");
var http = require("http");

var Q = require("q");

var konfig = {
    oauth: {
        mount: "/auth",
        host: "oauth.yandex-team.ru",
        authorize_path: "/authorize",
        token_path: "/token",
        client_id: "7dc2b061bd884693b520730cfb61b011",
        client_secret: "c494ca8a946a40f9bbdb36615b14551f",
        timeout: 3000
    },
    blackbox: {
        host: "blackbox.yandex-team.ru",
        path: "/blackbox",
        timeout: 3000,
        retries: 5,
        local: {
        }
    }
};

function makeHttpRequest(method, host, path, timeout, headers, body) {
    var deferred = Q.defer();
    var request = http.request({
        method : method,
        headers : headers,
        host : host,
        path : path
    });

    request.once("error", function(error) {
        deferred.reject(error);
    });
    request.once("response", function(response) {
        var chunks = [];
        response.on("data", function(chunk) { chunks.push(chunk); });
        response.on("end",  function() {
            deferred.resolve(chunks.join());
        });
    });
    request.setTimeout(timeout, function() {
        deferred.reject(new Error("Request timed out"));
    });
    request.setNoDelay(true);
    request.end(body);

    return deferred.promise;
}

function YtApplicationAuth(logger, global_config) { // TODO: Inject |config|
    var config = konfig.oauth;

    var template_index = mustache.compile(fs.readFileSync(
        __dirname + "/../static/auth-index.mustache").toString());
    var template_layout = mustache.compile(fs.readFileSync(
        __dirname + "/../static/auth-layout.mustache").toString());
    var template_token = mustache.compile(fs.readFileSync(
        __dirname + "/../static/auth-token.mustache").toString());
    var style = fs.readFileSync(__dirname + "/../static/bootstrap.min.css");

    function requestOAuthToken(code) {
        var deferred = Q.defer();
        var body = qs.stringify({
            grant_type : "authorization_code",
            code : code,
            client_id : config.client_id,
            client_secret : config.client_secret
        });
        var req = http.request({
            method : "POST",
            headers : {
                "Content-Type" : "application/www-form-urlencoded",
                "Content-Length" : body.length,
                "User-Agent" : "YT Authorization Application"
            },
            host : config.host,
            path : config.token_path
        });

        req.once("response", function(rsp) {
            var chunks = [];
            rsp.on("data",  function(chunk) { chunks.push(chunk); });
            rsp.on("end",   function() {
                try {
                    var data = JSON.parse(chunks.join());
                    if (data.access_token) {
                        deferred.resolve(data.access_token);
                    } else if (data.error) {
                        deferred.reject(new Error("OAuth server returned an error: " + data.error));
                    } else {
                        deferred.reject(new Error("OAuth server returned an invalid response: " + JSON.stringify(data)));
                    }
                } catch (ex) {
                    deferred.reject(new Error("OAuth server returned an invalid JSON"))
                }
            });
        });
        req.setTimeout(config.timeout, function() {
            deferred.reject(new Error("OAuth server timed out"));
        });
        req.setNoDelay(true);
        req.setSocketKeepAlive(false);
        req.end(body);

        return deferred.promise;
    }

    function httpRedirect(rsp, location, code) {
        rsp.statusCode = code;
        rsp.setHeader("Location", location);
        rsp.end();
    }

    function httpDispatch(rsp, body, type) {
        rsp.setHeader("Content-Length", typeof(body) === "string" ? Buffer.byteLength(body) : body.length);
        rsp.setHeader("Content-Type", type);
        rsp.end(body);
    }

    function handleIndex(req, rsp) {
        if (req.url === "/" && req.originalUrl.substr(-1) !== "/") {
            httpRedirect(rsp, req.originalUrl + "/", 301);
        } else {
            var body = template_layout({ content: template_index() });
            httpDispatch(rsp, body, "text/html; charset=utf-8");
        }
    }

    function handleNew(req, rsp) {
        var params = qs.parse(url.parse(req.url).query);
        if (params.code) {
            Q
                .when(requestOAuthToken(params.code),
                function(token) {
                    var body = template_layout({ content: template_token({ token: token })});
                    httpDispatch(rsp, body, "text/html; charset=utf-8");
                },
                function(error) {
                    var body = template_layout({ content: template_token({ error: error })});
                    httpDispatch(rsp, body, "text/html; charset=utf-8");
                });
        } else {
            var target = url.format({
                protocol : "http",
                host : config.host,
                pathname : config.authorize_path,
                query : {
                    response_type : "code",
                    client_id : config.client_id
                }
            });
            httpRedirect(rsp, target, 303);
        }
    }

    return function(req, rsp) {
        switch (url.parse(req.url).pathname) {
            case "/":
            case "/index":
                handleIndex(req, rsp); break;
            case "/new":
                handleNew(req, rsp); break;
            // There are only static routes below.
            case "/style":
                httpDispatch(rsp, style, "text/css");
                break;
        }
    };
};

exports.YtApplicationAuth = YtApplicationAuth;
