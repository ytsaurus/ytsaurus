var Q = require("bluebird");
var url = require("url");
var querystring = require("querystring");

var YtError = require("./error").that;
var YtRegistry = require("./registry").that;
var YtHttpRequest = require("./http_request").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

function generateMarker()
{
    return require("crypto").pseudoRandomBytes(16).toString("base64");
}

exports.blackboxValidateToken = function(logger, party, token)
{
    var config = YtRegistry.get("config", "services", "blackbox");
    var marker = generateMarker();

    return (function inner(retry)
    {
        var tagged_logger = new utils.TaggedLogger(
            logger,
            { retry: retry, blackbox_marker: marker });

        if (retry >= config.retries) {
            var error = new YtError("Too many failed Blackbox requests");
            tagged_logger.error(error.message, { retries: config.retries });
            return Q.reject(error);
        }

        tagged_logger.debug("Querying Blackbox to validate token");

        return new YtHttpRequest(
            config.host,
            config.port)
        .withPath(url.format({
            pathname: "/blackbox",
            query: {
                method: "oauth",
                format: "json",
                userip: party,
                oauth_token: token
            }
        }))
        .withHeader("X-YT-Marker", marker)
        .setNoDelay(config.nodelay)
        .setNoResolve(config.noresolve)
        .setTimeout(config.timeout)
        .asHttps(config.secure)
        .asJson(true)
        .shouldFailOn4xx(true)
        .shouldFailOn5xx(true)
        .fire()
        .then(function(data) {
            if (typeof(data.exception) !== "undefined") {
                var error = new YtError("Blackbox returned an exception");
                error.attributes.raw_data = JSON.stringify(data);
                tagged_logger.info(error.message, { data: data });
                return Q.reject(error);
            } else {
                tagged_logger.info(
                    "Successfully queried Blackbox",
                    { data: data });
                return data;
            }
        })
        .catch(function(err) {
            var error = YtError.ensureWrapped(err);
            tagged_logger.info("Retrying to query Blackbox", {
                // XXX(sandello): Embed.
                error: error.toJson()
            });
            return Q
            .delay(config.timeout * retry)
            .then(inner.bind(undefined, retry + 1));
        });
    })(0);
};

exports.blackboxValidateCookie = function(logger, party, sessionid, sslsessionid)
{
    var config = YtRegistry.get("config", "services", "blackbox");
    var marker = generateMarker();

    return (function inner(retry)
    {
        var tagged_logger = new utils.TaggedLogger(
            logger,
            { retry: retry, blackbox_marker: marker });

        if (retry >= config.retries) {
            var error = new YtError("Too many failed Blackbox requests");
            tagged_logger.error(error.message, { retries: config.retries });
            return Q.reject(error);
        }

        tagged_logger.debug("Querying Blackbox to validate cookie");

        var query = {
            method: "sessionid",
            format: "json",
            userip: party,
            host: config.host.split(".").slice(1).join("."),
        };
        if (sessionid) {
            query.sessionid = sessionid + "";
        }
        if (sslsessionid) {
            query.sslsessionid = sslsessionid + "";
        }

        return new YtHttpRequest(
            config.host,
            config.port)
        .withPath(url.format({
            pathname: "/blackbox",
            query: query,
        }))
        .withHeader("X-YT-Marker", marker)
        .setNoDelay(config.nodelay)
        .setNoResolve(config.noresolve)
        .setTimeout(config.timeout)
        .asHttps(config.secure)
        .asJson(true)
        .shouldFailOn4xx(true)
        .shouldFailOn5xx(true)
        .fire()
        .then(function(data) {
            if (typeof(data.exception) !== "undefined") {
                var error = new YtError("Blackbox returned an exception");
                error.attributes.raw_data = JSON.stringify(data);
                tagged_logger.info(error.message, { data: data });
                return Q.reject(error);
            } else {
                tagged_logger.info(
                    "Successfully queried Blackbox",
                    { data: data });
                return data;
            }
        })
        .catch(function(err) {
            var error = YtError.ensureWrapped(err);
            tagged_logger.info("Retrying to query Blackbox", {
                // XXX(sandello): Embed.
                error: error.toJson()
            });
            return Q
            .delay(config.timeout * retry)
            .then(inner.bind(undefined, retry + 1));
        });
    })(0);
};

exports.oAuthObtainToken = function(logger, client_id, client_secret, code)
{
    var config = YtRegistry.get("config", "services", "oauth");
    var marker = generateMarker();

    return (function inner(retry) {
        var tagged_logger = new utils.TaggedLogger(
            logger,
            { retry: retry, oauth_marker: marker });

        if (retry >= config.retries) {
            var error = new YtError("Too many failed OAuth requests");
            tagged_logger.error(error.message, { retries: config.retries });
            return Q.reject(error);
        }

        tagged_logger.debug("Querying OAuth");

        return new YtHttpRequest(
            config.host,
            config.port)
        .withVerb("POST")
        .withPath("/token")
        .withBody(querystring.stringify({
            code: code,
            grant_type: "authorization_code",
            client_id: client_id,
            client_secret: client_secret
        }), "application/www-form-urlencoded")
        .withHeader("X-YT-Marker", marker)
        .setNoDelay(config.nodelay)
        .setNoResolve(config.noresolve)
        .setTimeout(config.timeout)
        .asHttps(config.secure)
        .asJson(true)
        .shouldFailOn4xx(false)
        .shouldFailOn5xx(true)
        .fire()
        .then(
        function(data) {
            if (typeof(data.error) !== "undefined") {
                var error = new YtError(
                    "OAuth returned an error: " + data.error);
                error.attributes.raw_data = JSON.stringify(data);
                tagged_logger.info(error.message, { data: data });
                return Q.reject(error);
            } else {
                tagged_logger.info(
                    "Successfully queried OAuth",
                    { data: data });
                return data;
            }
        },
        function(err) {
            var error = YtError.ensureWrapped(err);
            tagged_logger.info("Retrying to query OAuth", {
                // XXX(sandello): Embed.
                error: error.toJson()
            });
            return Q
            .delay(config.timeout * retry)
            .then(inner.bind(undefined, retry + 1));
        });
    })(0);
};

exports.oAuthBuildUrlToRedirect = function(client_id, state)
{
    var config = YtRegistry.get("config", "services", "oauth");

    return url.format({
        protocol: "http",
        host: config.host,
        port: config.port,
        pathname: "/authorize",
        query: {
            response_type: "code",
            display: "popup",
            client_id: client_id,
            state: JSON.stringify(state)
        }
    });
};
