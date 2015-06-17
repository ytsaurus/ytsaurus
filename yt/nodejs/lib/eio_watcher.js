var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "EIO");

////////////////////////////////////////////////////////////////////////////////

function YtEioWatcher(logger, config) {
    this.logger = logger;
    this.thread_limit = config.thread_limit;
    this.spare_threads = config.spare_threads;

    __DBG("Concurrency: " + this.thread_limit + " (w/ " + this.spare_threads + " spare threads)");

    binding.SetEioConcurrency(this.thread_limit);
}

YtEioWatcher.prototype.tackle = function() {
    var info = binding.GetEioInformation();

    __DBG("Information: " + JSON.stringify(info));

    if (info.nthreads === this.thread_limit &&
        info.nthreads === info.nreqs && info.nready > 0)
    {
        this.logger.info("Eio is saturated; consider increasing thread limit", info);
    }
};

YtEioWatcher.prototype.is_choking = function() {
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
