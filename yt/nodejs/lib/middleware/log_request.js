var YtRegistry = require("../registry").that;

var utils = require("../utils");

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtLogRequest()
{
    "use strict";

    var logger = YtRegistry.get("logger");

    return function(req, rsp, next) {
        req._ts = new Date();
        req._bytes_in = 0;
        req._bytes_out = 0;

        if (req.logger) {
            return next();
        } else {
            req.logger = new utils.TaggedLogger(logger, {
                request_id: req.uuid,
                correlation_id: req.headers["x-yt-correlation-id"]
            });
        }

        var meta = {
            request_id     : req.uuid,
            correlation_id : req.headers["x-yt-correlation-id"],
            method         : req.method,
            url            : req.originalUrl,
            referrer       : req.headers["referer"] || req.headers["referrer"],
            remote_address : (req.connection && req.connection.remoteAddress) ||
                             (req.socket && req.socket.remoteAddress),
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

            logger.debug("Handled request", {
                request_id   : req.uuid,
                request_time : new Date() - req._ts,
                status_code  : rsp.statusCode,
                req_bytes    : req._bytes_in,
                rsp_bytes    : req._bytes_out
            });
        };

        next();
    };
};
