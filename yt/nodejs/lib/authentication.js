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

function YtBlackbox(logger, global_config) { // TODO: Inject |config|
    var config = konfig.blackbox;
    var locals = global_config.locals;
    var cache = lru_cache({ max: 5000, maxAge: 60 * 1000 /* ms */});

    function httpUnauthorized(rsp) {
        rsp.writeHead(401, { "WWW-Authenticate" : "OAuth scope=\"yt:api\"" });
        rsp.end();
    }

    function httpServiceUnavailable(rsp) {
        rsp.writeHead(503);
        rsp.end();
    }

    function requestOAuthAuthorization(token, ip, id, retry) {
        var uri = url.format({
            pathname : config.path,
            query : {
                method : "oauth",
                format : "json",
                oauth_token : token,
                userip : ip
            }
        });

        var cached = cache.get(token);
        if (cached !== undefined) {
            logger.debug("Blackbox cache hit", { request_id : id });
            return cache.get(token);
        } else {
            logger.debug("Blackbox cache miss", { request_id : id });
        }

        if (retry >= config.retries) {
            logger.error("Too many failed Blackbox requests (" + retry + "/" + config.retries + "); falling back.", { request_id : id });
            return Q.reject(new Error("Too many failed Blackbox requests (" + retry + ")"));
        }

        return Q
            .when(makeHttpRequest("GET", config.host, uri, config.timeout, {
                "User-Agent" : "YT Authorization Manager",
                "X-YT-Request-Id" : id
            }),
            function(data) {
                try {
                    data = JSON.parse(data);
                    logger.debug("Successfully received data from Blackbox", {
                        request_id : id,
                        retry : retry,
                        payload : data
                    });
                } catch (error) {
                    logger.debug("Failed to parse JSON data from Blackbox", {
                        request_id : id,
                        retry : retry,
                        error : error.toString()
                    });
                    return requestOAuthAuthorization(token, ip, id, retry + 1);
                }

                if (data.oauth && data.login && data.error) {
                    if (data.error === "OK") {
                      logger.debug("Blackbox has approved token; updating cache", {
                          request_id : id,
                          login : data.login
                      });
                      cache.set(token, data.login);
                      return data.login;
                    } else {
                      logger.debug("Blackbox has rejected token; invalidating cache", {
                          request_id : id,
                          error : data.error
                      });
                      cache.del(token);
                      return false;
                    }
                }

                if (data.exception) {
                    logger.info("Blackbox returned an exception", {
                        request_id : id,
                        error : data.exception
                    });
                    return requestOAuthAuthorization(token, ip, id, retry + 1);
                }

                logger.error("Unreachable", { request_id : id });
                return false;
            },
            function(error) {
                logger.error("Failed to query Blackbox", {
                    request_id : id,
                    retry : retry,
                    error : error.toString()
                });
                return requestOAuthAuthorization(token, ip, id, retry + 1);
            });
    }

    return function(req, rsp, next) {
        if (!req.headers.hasOwnProperty("authorization")) {
            logger.debug("Client is missing Authorization header", {
                request_id : req.uuid
            });
            return next();
        }

        var parts = req.headers["authorization"].split(/\s+/);
        var token = parts[1];

        req.authenticated_user = null;

        if (parts[0] !== "OAuth" || !token) {
            logger.debug("Client has improper Authorization header", {
                request_id : req.uuid,
                header : req.headers["authorization"]
            });
            return httpUnauthorized(rsp);
        }

        if (locals.hasOwnProperty(token) && locals[token]) {
            logger.debug("Client has been authenticated with local token", {
                request_id : req.uuid,
                login : locals[token]
            });
            req.authenticated_user = locals[token];
            return next();
        }

        var timestamp = new Date();

        Q
            .when(requestOAuthAuthorization(token, req.connection.remoteAddress, req.uuid, 0))
            .then(
            function(login) {
                var dt = (new Date()) - timestamp;
                if (!login) {
                    logger.debug("Client has failed to authenticate", {
                        request_id : req.uuid,
                        authentication_time : dt
                    });
                    httpUnauthorized(rsp);
                } else {
                    logger.debug("Client has been authenticated", {
                        request_id : req.uuid,
                        authentication_time : dt,
                        login : login
                    });
                    req.authenticated_user = login;
                    process.nextTick(next);
                }
            },
            function(error) {
                logger.debug("Client has not been authenticated but allowed to pass-through", {
                    request_id : req.uuid,
                    authentication_time : dt
                });
                process.nextTick(next);
            });
    }
};

function YtAuthenticationApplication(logger, global_config) { // TODO: Inject |config|
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

exports.YtAuthenticationApplication = YtAuthenticationApplication;
exports.YtBlackbox = YtBlackbox;
