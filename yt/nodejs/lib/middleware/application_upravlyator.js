var YtApplicationUpravlyator = require("../application_upravlyator").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationUpravlyator()
{
    "use strict";

    var config = YtRegistry.get("config", "upravlyator");
    var logger = YtRegistry.get("logger");
    var driver = YtRegistry.get("driver");

    return function(req, rsp, next) {
        return (new YtApplicationUpravlyator(
            config,
            req.logger || logger,
            driver))
        .dispatch(req, rsp, next);
    };
};
