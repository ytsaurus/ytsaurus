var uuid = require("node-uuid");

var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtMarkRequest() {
    "use strict";

    var fqdn = YtRegistry.get("config", "fqdn");
    var buffer = new Buffer(16);

    return function(req, rsp, next) {
        uuid.v4(null, buffer);
        req.uuid = buffer.toString("base64");

        rsp.setHeader("X-YT-Request-Id", req.uuid);
        rsp.setHeader("X-YT-Proxy", fqdn);

        var cid = req.headers["x-yt-correlation-id"];
        if (typeof(cid) !== "undefined") {
          rsp.setHeader("X-YT-Correlation-Id", cid);
        }

        next();
    };
};
