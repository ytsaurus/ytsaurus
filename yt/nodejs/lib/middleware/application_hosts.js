var Q = require("q");

var YtApplicationHosts = require("../application_hosts").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationHosts()
{
    "use strict";

    var logger = YtRegistry.get("logger");
    var coordinator = YtRegistry.get("coordinator");

    var app = new YtApplicationHosts(logger, coordinator);

    return function(req, rsp, next) {
        return Q(app.dispatch(req, rsp, next)).done();
    };
};
