var domain = require("domain");

var YtRegistry = require("../registry").that;
var YtError = require("../error").that;

var utils = require("../utils");

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtIsolateRequest()
{
    var logger = YtRegistry.get("logger");

    return function(req, rsp, next) {
        var rd = domain.create();
        rd.on("error", function(err) {
            var error = new YtError("Unhandled error in the request domain", err);
            var body = error.toJson();

            // This is bluntly copied from logging middleware.
            // Since domain error handling is a last resort, we are going deep.
            var request_id = req.uuid || "(unknown)";
            var socket_id = req.connection.uuid || "(unknown)";

            console.error("E   RID = " + request_id);
            console.error("E   SID = " + socket_id);
            console.error("EEE " + body);

            logger.error(error.message, {
                request_id: request_id,
                socket_id: socket_id,
                // TODO(sandello): Embed.
                error: body
            });

            rsp.statusCode = 500;
            utils.dispatchAs(rsp, body, "application/json");
            rd.dispose();
        });
        rd.run(next);
    };
};
