var YtApplicationAuth = require("../application_auth").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationAuth()
{
    "use strict";

    var config = YtRegistry.get("config", "authentication");
    var logger = YtRegistry.get("logger");
    var driver = YtRegistry.get("driver");

    var authority = YtRegistry.get("authority");

    return function(req, rsp, next) {
        return (new YtApplicationAuth(
            config,
            req.logger || logger,
            driver,
            authority))
        .dispatch(req, rsp, next);
    };
};
