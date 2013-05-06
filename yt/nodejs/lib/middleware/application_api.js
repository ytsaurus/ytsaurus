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
    var watcher = new YtEioWatcher(logger, config);
    var fqdn = config.fqdn;
    var read_only = config.read_only;

    return function(req, rsp, next) {
        return (new YtCommand(
            req.logger || logger,
            driver,
            watcher,
            fqdn,
            read_only,
            req.pauser
        )).dispatch(req, rsp);
    };
};
