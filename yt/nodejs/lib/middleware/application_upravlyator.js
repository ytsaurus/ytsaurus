var Q = require("bluebird");

var YtApplicationUpravlyator = require("../application_upravlyator").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationUpravlyator(driver, authority)
{
    var logger = YtRegistry.get("logger");
    
    driver = driver || YtRegistry.get("driver");
    authority = authority || YtRegistry.get("authority");
    var robot_yt_idm = YtRegistry.get("robot_yt_idm");

    var app = new YtApplicationUpravlyator(logger, driver, authority, robot_yt_idm);

    return function(req, rsp, next) {
        return Q.cast(app.dispatch(req, rsp, next)).done();
    };
};
