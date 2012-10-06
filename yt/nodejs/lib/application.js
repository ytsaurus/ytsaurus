var YtCommand = require("./command").that;
var YtDriver = require("./driver").that;
var YtEioWatcher = require("./eio_watcher").that;

var utils = require("./utils");
var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT(ALL|APP)/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Application:", x); };
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

exports.that = function YtApplication(logger, config) {
    "use strict";

    __DBG("New");

    binding.ConfigureSingletons(config.proxy);

    if (typeof(config.low_watermark) === "undefined") {
        config.low_watermark = parseInt(0.80 * config.memory_limit, 10);
    }

    if (typeof(config.high_watermark) === "undefined") {
        config.high_watermark = parseInt(0.95 * config.memory_limit, 10);
    }

    var driver = new YtDriver(false, config);
    var watcher = new YtEioWatcher(logger, config);
    var read_only = config.read_only;

    return function(req, rsp) {
        var pause = utils.Pause(req);
        return (new YtCommand(
            logger, driver, watcher, read_only, pause, req, rsp
        )).dispatch();
    };
};
