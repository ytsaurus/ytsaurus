var Q = require("q");

var YtApplicationUpravlyator = require("../application_upravlyator").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtApplicationUpravlyator()
{
    "use strict";

    var logger = YtRegistry.get("logger");
    var driver = YtRegistry.get("driver");

    var app = new YtApplicationUpravlyator(logger, driver);

    return function(req, rsp, next) {
        return Q(app.dispatch(req, rsp, next)).done();
    };
};
