var YtCommand = require("../command").that;
var YtEioWatcher = require("../eio_watcher").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function YtApplicationApi()
{
    "use strict";

    var config = YtRegistry.get("config");
    var logger = YtRegistry.get("logger");
    var driver = YtRegistry.get("driver");
    var coordinator = YtRegistry.get("coordinator");
    var watcher = new YtEioWatcher(logger, config);

    return function(req, rsp, next) {
        return (new YtCommand(
            req.logger || logger,
            driver,
            coordinator,
            watcher,
            req.pauser
        )).dispatch(req, rsp);
    };
};

