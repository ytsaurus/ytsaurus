var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./ytnode");

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
    this.partitioned_semaphore = {};

    binding.SetEioConcurrency(this.semaphore_limit);
}

YtEioWatcher.prototype.isChoking = function() {
    var info = binding.GetEioInformation();
    return false;
};

YtEioWatcher.prototype.acquireThread = function(user, command)
{
    var key = "user=" + user + ";command=" + command + ";";
    if (typeof(this.partitioned_semaphore[key]) !== "number") {
        this.partitioned_semaphore[key] = 0;
    }

    if (this.semaphore < this.semaphore_limit) {
        ++this.semaphore;
        ++this.partitioned_semaphore[key];
        this.profiler.set(
            "yt.http_proxy.concurrency_semaphore",
            {user: user, api_command: command},
            this.partitioned_semaphore[key]);
        return true;
    } else {
        return false;
    }
};

YtEioWatcher.prototype.releaseThread = function(user, command)
{
    var key = "user=" + user + ";command=" + command + ";";
    if (typeof(this.partitioned_semaphore[key]) !== "number") {
        this.partitioned_semaphore[key] = 0;
    }

    --this.semaphore;
    --this.partitioned_semaphore[key];
    this.profiler.set(
        "yt.http_proxy.concurrency_semaphore",
        {user: user, api_command: command},
        this.partitioned_semaphore[key]);
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtEioWatcher;
