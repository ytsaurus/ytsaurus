var lru_cache = require("lru-cache");

var YtCommand = require("../command").that;
var YtEioWatcher = require("../eio_watcher").that;
var YtRegistry = require("../registry").that;

////////////////////////////////////////////////////////////////////////////////

exports.that = function YtApplicationApi()
{
    var config = YtRegistry.get("config");
    var logger = YtRegistry.get("logger");
    var driver = YtRegistry.get("driver");
    var coordinator = YtRegistry.get("coordinator");
    var watcher = YtRegistry.get("eio_watcher");
    var sticky_cache = lru_cache({
        max: config.api.rate_check_cache_size,
        maxAge: config.api.rate_check_cache_age,
    });

    return function(req, rsp, next) {
        return (new YtCommand(
            req.logger || logger,
            driver,
            coordinator,
            watcher,
            sticky_cache,
            req.pauser
        )).dispatch(req, rsp);
    };
};

