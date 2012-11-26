var cluster = require("cluster");

////////////////////////////////////////////////////////////////////////////////

var __DBG;
var __DIE;

if (process.env.NODE_DEBUG && /YT(ALL|APP)/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Cluster Master:", x); };
    __DBG.$ = true;
} else {
    __DBG = function(){};
    __DBG.$ = false;
}

__DIE = function dieOfTrue(condition, message) {
    if (condition) {
        console.error("*** Aborting: " + message);
        process.abort();
    }
};

////////////////////////////////////////////////////////////////////////////////

function YtClusterHandle(logger, worker) {
    "use strict";

    this.logger     = logger;
    this.worker     = worker;
    this.state      = "unknown";
    this.young      = true;
    this.created_at = new Date();
    this.updated_at = new Date();
    this.timeout_at = null;

    this.postponeDeath(2000); // Initial startup should be fast; 2 seconds is enough.
};

YtClusterHandle.prototype.kill = function() {
    this.worker.send({ type : "gracefullyDie" });
};

YtClusterHandle.prototype.destroy = function() {
    if (this.timeout_at) {
        clearTimeout(this.timeout_at);
    }

    this.logger     = null;
    this.worker     = null;
    this.state      = "destroyed";
    this.updated_at = new Date();
    this.timeout_at = null;
};

YtClusterHandle.prototype.toString = function() {
    return require("util").format("<YtClusterHandle wid=%s pid=%s state=%s>",
        this.worker.id,
        this.worker.process.pid,
        this.state);
};

YtClusterHandle.prototype.handleMessage = function(message) {
    if (!message || !message.type) {
        return; // Improper message format.
    }

    this.postponeDeath(5000);

    switch (message.type) {
        case "heartbeat":
            break;
        case "alive":
        case "stopping":
        case "stopped":
            this.state = message.type;
            break;
        default:
            this.logger.warn("Received unknown message of type '" + message.type + "' from worker " + this.toString());
            break;
    }
};

YtClusterHandle.prototype.postponeDeath = function(timeout) {
    if (this.timeout_at) {
        clearTimeout(this.timeout_at);
    }

    this.updated_at = new Date();
    this.timeout_at = setTimeout(this.certifyDeath.bind(this), timeout);
};

YtClusterHandle.prototype.certifyDeath = function() {
    this.logger.error("Worker is dead", {
        wid : this.worker.id,
        pid : this.worker.process.pid,
        handle : this.toString()
    });

    try {
        this.worker.send("violentlyDie");
        this.worker.disconnect();
        this.worker.process.kill("SIGKILL");
    } catch (ex) {
    }

    this.destroy();
};

////////////////////////////////////////////////////////////////////////////////

function YtClusterMaster(logger, number_of_workers, cluster_options) {
    "use strict";

    __DBG("New");

    if (__DBG.$) {
        [
            "fork", "online", "listening", "disconnect", "exit"
        ].forEach(function(event) {
            cluster.on(event, function(worker) {
                __DBG(event + ": " + worker.id);
            });
        });
    }

    this.logger = logger;

    __DBG("Expected number of workers is " + number_of_workers);

    this.workers_expected = number_of_workers;
    this.workers_handles = {};

    this.timeout_for_respawn = null;
    this.timeout_for_shutdown = null;

    var self = this;

    cluster.on("exit", function(worker, code, signal) {
        __DIE(
            !self.workers_handles.hasOwnProperty(worker.id),
            "Received |message| event from the dead worker");

        self.logger.error("Worker has exited", {
            wid    : worker.id,
            pid    : worker.process.pid,
            code   : code,
            signal : signal
        });

        self.workers_handles[worker.id].certifyDeath();
        delete self.workers_handles[worker.id];

        self.scheduleRespawnWorkers();
    });

    cluster.setupMaster(cluster_options);
};

YtClusterMaster.prototype.kickstart = function() {
    while (this.countWorkers()[0] < this.workers_expected) {
        this.spawnWorker();
    }
};

YtClusterMaster.prototype.debug = function() {
    var  p;
    for (p in this.workers_handles) {
        var handle = this.workers_handles[p];
        console.error(" -> " + handle.toString());
    }
};

YtClusterMaster.prototype.countWorkers = function() {
    var  p, n_total = 0, n_young = 0;
    for (p in this.workers_handles) {
        if (this.workers_handles.hasOwnProperty(p)) {
            ++n_total;
            if (this.workers_handles[p].young) {
                ++n_young;
            }
        }
    }
    return [ n_total, n_young ];
};

YtClusterMaster.prototype.spawnWorker = function() {
    var worker = cluster.fork();
    var handle = this.workers_handles[worker.id] =
        new YtClusterHandle(this.logger, worker);

    worker.on("message", handle.handleMessage.bind(handle));
    this.logger.error("Spawned young worker", { handle : handle.toString() });
};

YtClusterMaster.prototype.killWorker = function() {
    var  p, handle;
    for (p in this.workers_handles) {
        if (this.workers_handles.hasOwnProperty(p)) {
            handle = this.workers_handles[p];
            if (!handle.young) {
                handle.kill();
                this.logger.error("Killed old worker", {
                    handle : handle.toString()
                });
            }
        }
    }
};

YtClusterMaster.prototype.respawnWorkers = function() {
    var current  = this.countWorkers();
    var n_total  = current[0];
    var n_young  = current[1];
    var n_target = this.workers_expected;

    // This method should decide whether a new worker is required and whether
    // a subsequent trailing check is required.
    var will_spawn = false;
    var will_kill = false;
    var will_reschedule = false;

    if (n_young === 0) {
        if (n_target > 0) {
            this.logger.info("Young generation is dead; resurrecting...");
            will_spawn = true;
            will_reschedule = true;
        }
    } else {
        if (n_young < n_total) {
            this.logger.info("Old generation is alive; killing...");
            will_kill = true;
            will_reschedule = false;
        }
        if (n_young < n_target) {
            this.logger.info("More young workers required; spawning...");
            will_spawn = true;
            will_reschedule = true;
        }
    }

    if (will_spawn) {
        this.spawnWorker();
    }

    if (will_kill) {
        this.killWorker();
    }

    if (will_reschedule) {
        this.scheduleRespawnWorkers();
    }
};

YtClusterMaster.prototype.scheduleRespawnWorkers = function() {
    if (this.timeout_for_respawn) {
        return;
    }

    var self = this;
    self.timeout_for_respawn = setTimeout(function() {
        self.timeout_for_respawn = null;
        self.respawnWorkers();
    }, 1000);
};

YtClusterMaster.prototype.restartWorkers = function() {
    this.logger.error("Starting rolling restart of workers");
    for (var i in this.workers_handles) {
        this.workers_handles[i].young = false;
    }
    this.scheduleRespawnWorkers();
};

YtClusterMaster.prototype.shutdownWorkers = function() {
    // NB: Rely an actual cluster state, not on |this.workers_handles|.
    this.logger.error("Starting graceful shutdown");
    for (var i in cluster.workers) {
        cluster.workers[i].send({ type : "gracefullyDie" });
    }
    for (var i in this.workers_handles) {
        this.workers_handles[i].young = false;
    }
    this.workers_expected = 0;
    setTimeout(this.shutdownWorkersLoop.bind(this), 1000);
};

YtClusterMaster.prototype.shutdownWorkersLoop = function() {
    // NB: Rely an actual cluster state, not on |this.workers_handles|.
    var n = Object.keys(cluster.workers).length;
    if (n > 0) {
        this.logger.error("There are " + n + " workers alive", { n : n });
        setTimeout(this.shutdownWorkersLoop.bind(this), 1000);
    } else {
        this.logger.error("All workers gone");
        process.exit();
    }
};

exports.that = YtClusterMaster;

