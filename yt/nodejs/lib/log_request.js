exports.that = function YtLogRequest(logger) {
    "use strict";

    return function(req, rsp, next) {
        req._startTime = new Date();
        req._bytesIn = 0;
        req._bytesOut = 0;

        if (req._logging) {
            return next();
        } else {
            req._logging = true;
        }

        logger.info("Handling request", {
            request_id  : req.uuid,
            method      : req.method,
            url         : req.originalUrl,
            referrer    : req.headers["referer"] || req.headers["referrer"],
            remote_addr : req.socket && (req.socket.remoteAddress || (req.socket.socket && req.socket.socket.remoteAddress)),
            user_agent  : req.headers["user-agent"]
        });

        req.on("data", function(chunk) {
            req._bytesIn += chunk.length;
        });

        var write = rsp.write;
        rsp.write = function(chunk, encoding) {
            req._bytesOut += chunk.length;
            return write.apply(this, arguments);
        };

        var end = rsp.end;
        rsp.end = function(chunk, encoding) {
            rsp.end = end;
            rsp.end(chunk, encoding);

            logger.info("Handled request", {
                request_id   : req.uuid,
                request_time : new Date() - req._startTime,
                status       : rsp.statusCode,
                bytes_in     : req._bytesIn,
                bytes_out    : req._bytesOut
            });
        };

        next();
    };
};
