var crypto = require("crypto");

var YtRegistry = require("../registry").that;

var utils = require("../utils");

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtLogRequest()
{
    var fqdn = YtRegistry.get("fqdn");
    var logger = YtRegistry.get("logger");
    var profiler = YtRegistry.get("profiler");
    var buffer = new Buffer(16);

    return function(req, rsp, next) {
        req.uuid_ui64 = crypto.pseudoRandomBytes(8);
        req.uuid = req.uuid_ui64.toString("hex");

        req.tags = {};

        req.connection.last_request_id = req.uuid;

        // Store useful information.
        var request_id = req.uuid;
        var socket_id = req.connection.uuid;
        var correlation_id = req.headers["x-yt-correlation-id"];
        var x_forwarded_for = req.headers["x-forwarded-for"];

        // We are actually keeping tagged logger lean.
        req.logger = new utils.TaggedLogger(logger, { request_id: request_id });

        if (typeof(x_forwarded_for) !== "undefined") {
            req.origin = x_forwarded_for.split(',')[0];
            req.xff = x_forwarded_for;
        } else {
            req.origin = req.connection.remoteAddress;
            req.xff = null;
        }

        req._ts = new Date();
        req._bytes_in = 0;
        req._bytes_out = 0;

        // Make client aware of our identificators.
        rsp.setHeader("X-YT-Proxy", fqdn);
        rsp.setHeader("X-YT-Request-Id", request_id);

        if (typeof(socket_id) !== "undefined") {
            rsp.setHeader("X-YT-Socket-Id", socket_id);
        }

        if (typeof(correlation_id) !== "undefined") {
            rsp.setHeader("X-YT-Correlation-Id", correlation_id);
        }

        // Log all useful information.
        var meta = {
            // To avoid extra indirection through tagged logger.
            request_id     : request_id,
            socket_id      : socket_id,
            correlation_id : correlation_id,
            method         : req.method,
            url            : req.originalUrl || req.url,
            origin         : req.origin || req.connection.remoteAddress,
            referrer       : req.headers["referer"] || req.headers["referrer"],
            user_agent     : req.headers["user-agent"]
        };

        // Log all "X-" headers.
        var headers = Object.keys(req.headers);
        for (var i = 0, n = headers.length; i < n; ++i) {
            if (headers[i].indexOf("x-") === 0) {
                meta[headers[i]] = req.headers[headers[i]];
            }
        }

        logger.debug("Handling request", meta);

        req.on("data", function(chunk) {
            req._bytes_in += chunk.length;
        });

        var write = rsp.write;
        rsp.write = function(chunk, encoding) {
            req._bytes_out += chunk.length;
            return write.apply(this, arguments); // NB: 'return' is crucial here.
        };

        var end = rsp.end;
        rsp.end = function(chunk, encoding) {
            if (chunk) {
                req._bytes_out += chunk.length;
            }
            rsp.end = end;
            rsp.end(chunk, encoding);
            var request_time = new Date() - req._ts;
            logger.debug("Handled request", {
                request_id     : request_id,
                request_time   : request_time,
                socket_id      : socket_id,
                correlation_id : correlation_id,
                status_code    : rsp.statusCode,
                req_bytes      : req._bytes_in,
                rsp_bytes      : req._bytes_out
            });
            var tags = req.tags;
            tags.http_code = rsp.statusCode;
            profiler.inc("yt.http_proxy.request_count", tags, 1);
            profiler.inc("yt.http_proxy.request_bytes_in", tags, req._bytes_in);
            profiler.inc("yt.http_proxy.request_bytes_out", tags, req._bytes_out);
        };

        next();
    };
};
