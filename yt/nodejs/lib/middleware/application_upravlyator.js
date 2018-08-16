var Q = require("bluebird");

var YtApplicationUpravlyator = require("../application_upravlyator").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationUpravlyator()
{
    var logger = YtRegistry.get("logger");
    var driver = YtRegistry.get("driver");
    var authority = YtRegistry.get("authority");
    var robot_yt_idm = YtRegistry.get("robot_yt_idm");

    var app = new YtApplicationUpravlyator(logger, driver, authority, robot_yt_idm);

    return function(req, rsp, next) {
        return Q.cast(app.dispatch(req, rsp, next)).done();
    };
};
