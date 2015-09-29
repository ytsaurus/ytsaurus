var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "EIO");

////////////////////////////////////////////////////////////////////////////////

function YtEioWatcher(logger, config) {
    this.logger = logger;
    this.thread_limit = config.thread_limit;
    this.spare_threads = config.spare_threads;
    this.semaphore_limit = config.concurrency_limit || config.thread_limit;
    this.semaphore = 1; // Preallocate a thread for internal needs.

    binding.SetEioConcurrency(this.thread_limit);
}

YtEioWatcher.prototype.isChoking = function() {
    var info = binding.GetEioInformation();

    if (this.thread_limit - this.spare_threads <= info.nreqs - info.npending) {
        __DBG("We are choking!");
        return true;
    } else {
        return false;
    }
};

YtEioWatcher.prototype.acquireThread = function() {
    if (this.semaphore < this.semaphore_limit) {
        ++this.semaphore;
        return true;
    } else {
        return false;
    }
};

YtEioWatcher.prototype.releaseThread = function() {
    --this.semaphore;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtEioWatcher;
