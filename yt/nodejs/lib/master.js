var cluster = require("cluster");

////////////////////////////////////////////////////////////////////////////////

var __DBG;
var __DIE;

if (process.env.NODE_DEBUG && /YT(ALL|CLUSTER)/.test(process.env.NODE_DEBUG)) {
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

    this.__cached_wid = undefined;
    this.__cached_pid = undefined;

    this.postponeDeath(5000); // Initial startup should be fast; 5 seconds is enough.
};

YtClusterHandle.prototype.getWid = function() {
    if (!this.__cached_wid) {
        this.__cached_wid = this.worker ? this.worker.id : -1;
    } else {
        return this.__cached_wid;
    }
};

YtClusterHandle.prototype.getPid = function() {
    if (!this.__cached_pid) {
        this.__cached_pid = this.worker ? this.worker.process.pid : -1;
    } else {
        return this.__cached_pid;
    }
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
        this.getWid(),
        this.getPid(),
        this.state);
};

YtClusterHandle.prototype.handleMessage = function(message) {
    if (!message || !message.type) {
        return; // Improper message format.
    }

    if (this.state === "alive") {
        this.postponeDeath(30000);
    }

    switch (message.type) {
        case "heartbeat":
            if (!__DBG.$) {
                this.worker.send({ type : "heartbeat" });
            }
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
    if (__DBG.$) {
        return;
    }

    if (this.timeout_at) {
        clearTimeout(this.timeout_at);
    }

    this.updated_at = new Date();
    this.timeout_at = setTimeout(this.ageToDeath.bind(this), timeout);
};

YtClusterHandle.prototype.ageToDeath = function() {
    this.logger.info("Worker is not responding", {
        wid : this.getWid(),
        pid : this.getPid(),
        handle : this.toString()
    });
    this.certifyDeath();
}

YtClusterHandle.prototype.certifyDeath = function() {
    try {
        this.logger.info("Worker is dead", {
            wid : this.getWid(),
            pid : this.getPid(),
            handle : this.toString()
        });

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

    cluster.on("fork", function(worker) {
        self.workers_handles[worker.id].getPid();
        self.workers_handles[worker.id].getWid();
    });

    cluster.on("exit", function(worker, code, signal) {
        __DIE(
            !self.workers_handles.hasOwnProperty(worker.id),
            "Received |message| event from the dead worker");

        self.logger.info("Worker has exited", {
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
        this.spawnNewWorker();
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

YtClusterMaster.prototype.spawnNewWorker = function() {
    var worker = cluster.fork();
    var handle = this.workers_handles[worker.id] =
        new YtClusterHandle(this.logger, worker);

    worker.on("message", handle.handleMessage.bind(handle));
    this.logger.info("Spawned young worker");
};

YtClusterMaster.prototype.killOldWorker = function() {
    var  p, handle;
    for (p in this.workers_handles) {
        if (this.workers_handles.hasOwnProperty(p)) {
            handle = this.workers_handles[p];
            if (!handle.young) {
                handle.kill();
                this.logger.info("Killed old worker", {
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
        this.spawnNewWorker();
    }

    if (will_kill) {
        this.killOldWorker();
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
    this.logger.info("Starting rolling restart of workers");
    for (var i in this.workers_handles) {
        this.workers_handles[i].young = false;
    }
    this.scheduleRespawnWorkers();
};

YtClusterMaster.prototype.shutdownWorkers = function() {
    // NB: Rely an actual cluster state, not on |this.workers_handles|.
    this.logger.info("Starting graceful shutdown");
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
        this.logger.info("There are " + n + " workers alive", { n : n });
        setTimeout(this.shutdownWorkersLoop.bind(this), 1000);
    } else {
        this.logger.info("All workers gone");
        process.exit();
    }
};

exports.that = YtClusterMaster;

