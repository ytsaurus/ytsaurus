var url = require("url");
var qs = require("querystring");
var fs = require("fs");
var lru_cache = require("lru-cache");
var connect = require("connect");
var mustache = require("mustache");
var http = require("http");

var Q = require("q");

var konfig = {
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
    var disabled = global_config.disable_blackbox;
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
            return cached;
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
                          retry : retry,
                          login : data.login
                      });
                      cache.set(token, data.login);
                      return data.login;
                    } else {
                      logger.debug("Blackbox has rejected token; invalidating cache", {
                          request_id : id,
                          retry : retry,
                          error : data.error
                      });
                      cache.del(token);
                      return false;
                    }
                }

                if (data.exception) {
                    logger.info("Blackbox returned an exception", {
                        request_id : id,
                        retry : retry,
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

    // TODO(sandello): Configure user names from config.
    return function(req, rsp, next) {
        if (disabled) {
            req.authenticated_user = "root";
            return next();
        }

        if (!req.headers.hasOwnProperty("authorization")) {
            logger.debug("Client is missing Authorization header", {
                request_id : req.uuid
            });
            var ua = req.headers["user-agent"];
            if (ua && ua.indexOf("Python wrapper") === 0) {
                req.authenticated_user = undefined;
                return httpUnauthorized(rsp);
            } else {
                req.authenticated_user = "guest";
                return next();
            }
        }

        var parts = req.headers["authorization"].split(/\s+/);
        var token = parts[1];

        if (parts[0] !== "OAuth" || !token) {
            logger.debug("Client has improper Authorization header", {
                request_id : req.uuid,
                header : req.headers["authorization"]
            });
            req.authenticated_user = undefined;
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
                req.authenticated_user = "guest";
                process.nextTick(next);
            });
    }
};

exports.that = YtBlackbox;
