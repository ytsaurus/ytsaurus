var Q = require("bluebird");

var YtApplicationAuth = require("../application_auth").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationAuth()
{
    var config = YtRegistry.get("config", "authentication");
    var logger = YtRegistry.get("logger");
    var profiler = YtRegistry.get("profiler");
    var authority = YtRegistry.get("authority");

    var app = new YtApplicationAuth(config, logger, profiler, authority);

    return function(req, rsp, next) {
        return Q.cast(app.dispatch(req, rsp, next)).done();
    };
};
