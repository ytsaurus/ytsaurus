var uuid = require("node-uuid");

////////////////////////////////////////////////////////////////////////////////

exports.that = function YtAssignRequestId() {
    "use strict";

    var buffer = new Buffer(16);

    return function(req, rsp, next) {
        uuid.v4(null, buffer);

        req.uuid = buffer.toString("base64");
        rsp.setHeader("X-YT-Request-Id", req.uuid);

        next();
    };
};
