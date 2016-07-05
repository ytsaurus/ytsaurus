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
<<<<<<< HEAD
=======

    this.partitioned_semaphore = {};
>>>>>>> origin/prestable/18.4

    binding.SetEioConcurrency(this.semaphore_limit);
}

YtEioWatcher.prototype.isChoking = function() {
    var info = binding.GetEioInformation();
    return false;
};
<<<<<<< HEAD
=======

YtEioWatcher.prototype.acquireThread = function(user, command)
{
    var key = "user=" + user + ";command=" + command + ";";
    if (typeof(this.partitioned_semaphore[key]) !== "number") {
        this.partitioned_semaphore[key] = 0;
    }
>>>>>>> origin/prestable/18.4

    if (this.semaphore < this.semaphore_limit) {
        ++this.semaphore;
<<<<<<< HEAD
        this.profiler.set("yt.http_proxy.concurrency_semaphore", tags, this.semaphore);
=======
        ++this.partitioned_semaphore[key];
        this.profiler.set(
            "yt.http_proxy.concurrency_semaphore",
            {user: user, api_command: command},
            this.partitioned_semaphore[key]);
>>>>>>> origin/prestable/18.4
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
<<<<<<< HEAD
    this.profiler.set("yt.http_proxy.concurrency_semaphore", tags, this.semaphore);
=======
    --this.partitioned_semaphore[key];
    this.profiler.set(
        "yt.http_proxy.concurrency_semaphore",
        {user: user, api_command: command},
        this.partitioned_semaphore[key]);
>>>>>>> origin/prestable/18.4
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtEioWatcher;
