var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YTAPP/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Eio:", x); };
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

function YtEioWatcher(logger, configuration) {
    "use strict";
    this.logger = logger;
    this.thread_limit = configuration.thread_limit;
    this.spare_threads = configuration.spare_threads;

    __DBG("Concurrency: " + this.thread_limit + " (w/ " + this.spare_threads + " spare threads)");

    binding.SetEioConcurrency(this.thread_limit);
}

YtEioWatcher.prototype.tackle = function() {
    "use strict";
    var info = binding.GetEioInformation();

    __DBG("Information: " + JSON.stringify(info));

    if (info.nthreads === this.thread_limit &&
        info.nthreads === info.nreqs && info.nready > 0)
    {
        this.logger.info("Eio is saturated; consider increasing thread limit", info);
    }
};

YtEioWatcher.prototype.is_choking = function() {
    "use strict";
    var info = binding.GetEioInformation();

    if (this.thread_limit - this.spare_threads <= info.nreqs - info.npending) {
        __DBG("We are choking!");
        return true;
    } else {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtEioWatcher;
