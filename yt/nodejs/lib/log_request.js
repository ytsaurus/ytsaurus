exports.that = function YtLogRequest(logger) {
    "use strict";

    return function(req, rsp, next) {
        req._start_time = new Date();
        req._bytes_in = 0;
        req._bytes_out = 0;

        if (req._logging) {
            return next();
        } else {
            req._logging = true;
        }

        logger.info("Handling request", {
            request_id     : req.uuid,
            method         : req.method,
            url            : req.originalUrl,
            referrer       : req.headers["referer"] || req.headers["referrer"],
            remote_address : req.socket && (req.socket.remoteAddress || (req.socket.socket && req.socket.socket.remoteAddress)),
            user_agent     : req.headers["user-agent"]
        });

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

            logger.info("Handled request", {
                request_id   : req.uuid,
                request_time : new Date() - req._start_time,
                status       : rsp.statusCode,
                req_bytes    : req._bytes_in,
                rsp_bytes    : req._bytes_out
            });
        };

        next();
    };
};
