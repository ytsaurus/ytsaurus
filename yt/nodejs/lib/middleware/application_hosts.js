var Q = require("bluebird");

var YtApplicationHosts = require("../application_hosts").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationHosts()
{
    var logger = YtRegistry.get("logger");
    var coordinator = YtRegistry.get("coordinator");
    var show_ports = YtRegistry.get("config", "show_ports");

    var app = new YtApplicationHosts(logger, coordinator, show_ports);

    return function(req, rsp, next) {
        return Q.cast(app.dispatch(req, rsp, next)).done();
    };
};
