var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "EIO");

////////////////////////////////////////////////////////////////////////////////

function YtEioWatcher(logger, profiler, config) {
    this.logger = logger;
    this.profiler = profiler;
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

YtEioWatcher.prototype.acquireThread = function(tags) {
    if (this.semaphore < this.semaphore_limit) {
        this.profiler.inc("yt.http_proxy.concurrency_semaphore", tags, 1);
        ++this.semaphore;
        return true;
    } else {
        return false;
    }
};

YtEioWatcher.prototype.releaseThread = function(tags) {
    --this.semaphore;
    this.profiler.inc("yt.http_proxy.concurrency_semaphore", tags, -1);
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtEioWatcher;
