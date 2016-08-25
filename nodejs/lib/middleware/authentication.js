var YtAuthentication = require("../authentication").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtAuthentication()
{
    var config = YtRegistry.get("config", "authentication");
    var logger = YtRegistry.get("logger");
    var profiler = YtRegistry.get("profiler");
    var authority = YtRegistry.get("authority");

    return function(req, rsp, next) {
        return (new YtAuthentication(
            config,
            req.logger || logger,
            profiler,
            authority))
        .dispatch(req, rsp, next);
    };
};
