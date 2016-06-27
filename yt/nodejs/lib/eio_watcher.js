var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "EIO");

////////////////////////////////////////////////////////////////////////////////

function YtEioWatcher(logger, profiler, config) {
    this.logger = logger;
    this.profiler = profiler;
    this.thread_limit = config.thread_limit;
    this.spare_threads = config.spare_threads;

    this.reserve = 1 + config.spare_threads;
    this.semaphore = 0;
    this.semaphore_limit = config.concurrency_limit || config.thread_limit;
    this.semaphore_limit = 2 * this.semaphore_limit + this.semaphore - this.reserve;

    binding.SetEioConcurrency(this.semaphore_limit);
}

YtEioWatcher.prototype.isChoking = function() {
    var info = binding.GetEioInformation();
    return false;
};

YtEioWatcher.prototype.acquireThread = function(tags) {
    if (this.semaphore < this.semaphore_limit) {
        ++this.semaphore;
        this.profiler.set("yt.http_proxy.concurrency_semaphore", tags, this.semaphore);
        return true;
    } else {
        return false;
    }
};

YtEioWatcher.prototype.releaseThread = function(tags) {
    --this.semaphore;
    this.profiler.set("yt.http_proxy.concurrency_semaphore", tags, this.semaphore);
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtEioWatcher;
