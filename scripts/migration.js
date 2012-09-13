#!/usr/bin/env node
// npm install winston

var fs = require("fs");
var path = require("path");
var child_process = require("child_process");
var util = require("util");

var async = require("async");
var charm = require("charm")();
var moment = require("moment");
var node_uuid = require("node-uuid");
var underscore = require("underscore");
var winston = require("winston");

var $ = async.noConflict();
var _ = underscore.noConflict();

// TODO
// - Try https://github.com/kriskowal/q/
// - Write documntation
// - Implement proper CLI interface
// - Better progress bar & time estimation via LS

////////////////////////////////////////////////////////////////////////////////

var __UUID        = node_uuid.v4();
var __DATE        = new Date();
var __CONCURRENCY = 24;
var __SEPARATOR   = "----------";

__SEPARATOR = __SEPARATOR + __SEPARATOR; // 20
__SEPARATOR = __SEPARATOR + __SEPARATOR; // 40
__SEPARATOR = __SEPARATOR + __SEPARATOR; // 80

////////////////////////////////////////////////////////////////////////////////

var logger = new winston.Logger({
    levels: winston.config.syslog.levels,
    transports: [
        new winston.transports.Console({
            level: "error",
            colorize: true,
            timestamp: false
        }),
        new winston.transports.File({
            level: "debug",
            filename: "migration-" + moment().format("YYYYMMDD-HHmm") + ".log",
            timestamp: true
        })
    ]
});

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

function Yt(executable, libraries, configuration) {
    this.executable = executable;
    this.libraries = libraries;
    this.configuration = configuration;

    this.bundle = this.libraries.concat(this.executable, this.configuration);
    this.storage = [ "/", "tmp", "-migration-" + __UUID ];
}

Yt.prototype.spawn = function() {
    var args = _(arguments).values();
    var uuid = node_uuid.v4();

    var bound_logger = function() {
        logger.debug("(Child " + uuid + ") " + util.format.apply(this, arguments));
    };

    bound_logger("Spawning YT %s", util.inspect(args));

    var child = null;
    try {
        child = child_process.spawn(this.executable, args, {
            env : {
                "LD_LIBRARY_PATH" : path.dirname(this.executable),
                "YT_CONFIG" : this.configuration,
                "YT_MIGRATION" : 1
            }
        });
    } catch(ex) {
        logger.error("Failed to spawn a child (" + util.inspect(args) + "): " + ex.toString());
        logger.close();
        process.abort();
    }

    var stderr = "";

    child.stderr.on("data", function(chunk) {
        stderr += chunk.toString();
    });

    child.stderr.on("end", function() {
        if (stderr) {
            bound_logger("Captured STDERR:\n%s\n%s\n%s", __SEPARATOR, stderr, __SEPARATOR);
        }
    });

    child.on("exit", function(code) {
        bound_logger("Process terminated with exit code %d", code);
    });

    return child;
};

Yt.prototype.communicate = function(options, cb) {
    var args = options.arguments || [];

    var ignore_stdout = options.ignore_stdout || false;
    var ignore_stderr = options.ignore_stderr || false;

    var stdout = "";
    var stderr = "";
    var complete_stdout = false;
    var complete_stderr = false;
    var exit_code = null;

    var cb_if_needed = function cbIfNeeded() {
        if (
            !(ignore_stdout || complete_stdout) ||
            !(ignore_stderr || complete_stderr) ||
            exit_code === null
        ) {
            return;
        }

        if (exit_code !== 0) {
            var error = util.format("Process terminated with non-zero exit code %d", exit_code);
            return cb(new Error(error));
        }

        return cb(null, stdout, stderr);
    };

    var child = this.spawn.apply(this, args.concat("--format", "json"));

    if (!ignore_stdout) {
        child.stdout.on("data", function(chunk) {
            stdout += chunk.toString();
        });

        child.stdout.on("end", function(chunk) {
            complete_stdout = true;
            cb_if_needed();
        });
    }

    if (!ignore_stderr) {
        child.stderr.on("data", function(chunk) {
            stderr += chunk.toString();
        });

        child.stderr.on("end", function(chunk) {
            complete_stderr = true;
            cb_if_needed();
        });
    }

    child.on("exit", function(code) {
        exit_code = code;
        cb_if_needed();
    });

    return child;
}

Yt.prototype.execute = function() {
    var args = _(arguments).first(-1);
    var cb   = _(arguments).last();

    return this.communicate({
        arguments: args,
        ignore_stdout: false,
        ignore_stderr: false,
    }, function(err, stdout, stderr) {
        if (err) {
            return cb(err);
        }

        var result;
        try {
            result = stdout.length > 0 ? JSON.parse(stdout) : null;
        } catch(ex) {
            var error = util.format("Failed to parse result %s: %s", JSON.stringify(stdout), ex.toString());
            return cb(new Error(error));
        }

        return cb(null, result);
    });
};

////////////////////////////////////////////////////////////////////////////////

function renderTitle() {
    charm.display("reset");
    charm.display("bright").write("\n");
    charm.write(util.format.apply(null, arguments));
    charm.write("\n\n").display("reset");
}

function renderSubtitle() {
    charm.display("reset");
    charm.write(util.format.apply(null, arguments));
    charm.write("\n\n").display("reset");
}

function renderProgressBar(fraction, number_of_completed, number_of_total) {
    var width_total = 80;
    var width_completed = number_of_total === 0 ? 0 : Math.round(width_total * (number_of_completed / number_of_total));

    charm.push(true);

    charm.erase("line");
    charm.display("bright").write("[").display("reset");
    charm.background("blue").foreground("white");
    charm.write(Array(width_completed + 1).join("#"));
    charm.display("reset");
    charm.write(Array(width_total - width_completed + 1).join(" "));
    charm.display("bright").write("]").display("reset");
    charm.write(util.format(" %s / %s\n", number_of_completed, number_of_total));

    charm.pop(true);
}

function Semaphore(initial_tasks, initial_weight, cb) {
    this.started_at = moment();

    this.active_tasks = this.total_tasks = initial_tasks;
    this.active_weight = this.total_weight = initial_weight;

    _.bindAll(this);

    this.cb = cb || (function() {});

    if (this.total_tasks > 0) {
        this.redraw();
    }
}

Semaphore.prototype.redraw = function() {
    renderProgressBar(
        1.0 - this.active_weight / this.total_weight, 
        this.total_tasks - this.active_tasks, this.total_tasks);
};

Semaphore.prototype.increment = function() {
    var delta_tasks = arguments.length > 0 ? arguments[0] : 1;
    var delta_weight = arguments.length > 1 ? arguments[1] : 1;

    this.active_tasks += delta_tasks;
    this.active_weight += delta_weight
    this.total_tasks += delta_tasks;
    this.total_weight += delta_weight;

    this.redraw();
};

Semaphore.prototype.decrement = function() {
    var delta_tasks = arguments.length > 0 ? arguments[0] : 1;
    var delta_weight = arguments.length > 1 ? arguments[1] : 1;

    this.active_tasks -= delta_tasks;
    this.active_weight -= delta_weight

    this.redraw();

    if (this.active_tasks <= 0) {
        charm.write("\n\n  (Completed in " + moment.duration(moment() - this.started_at).asSeconds() + "s)\n\n");
        this.cb.apply(null, arguments);
    }
};

////////////////////////////////////////////////////////////////////////////////

function ypathJoin(path) {
    return path.map(ypathEscape).join('/');
}

function ypathEscape(token) {
    return token === '/' || token[0] === '@' ? token : '"' + token + '"';
}

function hasPrefix(s, p) {
    for (var i = 0; i < s.length && i < p.length; ++i) {
        if (s[i] !== p[i]) {
            return false;
        }
    }
    return i === p.length;
}

function removePrefix(s, p) {
    return s.slice(p.length);
}

////////////////////////////////////////////////////////////////////////////////
// A few comments for code below. There is a lot of inline callbacks and control
// flow is quite tricky to grasp from the first sight. Therefore I decided to 
// use "return" statements to explicitly mark normal exit points.
// Note that there are also "process.abort" calls which are essentialy abnormal
// exit points.

function YtController(source, target) {
    this.source = source;
    this.target = target;

    _.bindAll(this.source);
    _.bindAll(this.target);

    this.source_path_prefix = [ "/" ];
    this.target_path_prefix = [ "/", "backup" ];

    this.nodes = [];

    charm.pipe(process.stdout);
    charm.reset();
};

YtController.SUPPORTED_TYPES = [ "map_node", "file", "table" ];
YtController.IGNORED_PATHS = [ [ "sys" ], [ "tmp" ] ];

YtController.prototype._isSupportedType = function(type) {
    return _.chain(YtController.SUPPORTED_TYPES).indexOf(type).value() !== -1;
}

YtController.prototype._isIgnoredPath = function(path) {
    return _.chain(YtController.IGNORED_PATHS)
        .map(_.bind(hasPrefix, null, path))
        .any()
        .value();
}

YtController.behaviour = {
    "map_node": {
        doVerify: function(source_attributes, target_attributes) {
            return true;
        },
        doCopy: function(source_attributes, source_path, target_attributes, target_path) {
        }
    },
    "file": {
        doVerify: function(source_attributes, target_attributes) {
            return source_attributes.size === target_attributes.size;
        }
    }
}

YtController.prototype._traverseSourceCypress = function(doVisit, cb) {
    var self = this;
    var semaphore = new Semaphore(1, 1, cb);

    var queue = $.queue(function(path, cb) {
        var ypath = ypathJoin(path);

        self.source.execute("get", ypath + "/@", function(err, attributes) {
            if (err) {
                logger.error("Failed to obtain type for source node '" + ypath + "': " + err.toString());
                logger.close();
                process.abort();
            }

            if (!self._isSupportedType(attributes.type)) {
                logger.error("Source node '" + ypath + "' has unsupported type '" + attributes.type + "'; aborting");
                logger.close();
                process.abort();
            }

            if (self._isIgnoredPath(removePrefix(path, self.source_path_prefix))) {
                logger.debug("Source node '" + ypath + "' is ignored; skipping");
                return cb();
            }

            logger.debug("Visiting '" + ypath + "'");
            doVisit(path, attributes);

            switch (attributes.type) {
                case "map_node":
                case "list_node":
                    self.source.execute("list", ypath, function(err, descendants) {
                        if (err) {
                            logger.error("Failed to descend into source node '" + ypath + "': " + err.toString());
                            logger.close();
                            process.abort();
                        }

                        semaphore.increment(descendants.length);
                        queue.push(
                            descendants.map(function(descendant) { return path.concat(descendant); }),
                            semaphore.decrement);
                        return cb();
                    });
                    break;
                default:
                    return cb();
                    break;
            }
        });
    }, __CONCURRENCY);

    queue.push([ self.source_path_prefix ], semaphore.decrement);
}

YtController.prototype.gatherNodes = function(cb) {
    renderTitle("(1) Gathering nodes from Cypress");
    var self = this;

    self._traverseSourceCypress(
        function(path, attributes) {
            var node = {
                path: removePrefix(path, self.source_path_prefix),
                source_attributes: attributes,
                target_attributes: undefined
            };
            node.source_path = self.source_path_prefix.concat(node.path);
            node.source_ypath = ypathJoin(node.source_path);
            node.target_path = self.target_path_prefix.concat(node.path);
            node.target_ypath = ypathJoin(node.target_path);
            self.nodes.push(node);
        },
        function() {
            logger.info(util.format("We have %d nodes to migrate", self.nodes.length));
            return cb.apply(null, arguments);
        });
}

YtController.prototype.examineNodes = function(cb) {
    var self = this;
    renderTitle("(2) Examining %d nodes before migration", self.nodes.length);
    var semaphore = new Semaphore(self.nodes.length, self.nodes.length);

    $.forEachLimit(self.nodes, __CONCURRENCY, function(node, cb_) {
        var cb = function() { semaphore.decrement(); cb_.apply(null, arguments); };

        self.target.execute("get", node.target_ypath + "/@", function(err, attributes) {
            node.target_attributes = attributes;

            if (err) {
                return cb();
            } else {
                logger.debug("Target node '" + node.target_ypath + "' already exists");
                if (node.source_attributes.type != node.target_attributes.type) {
                    logger.error("Source node '" + node.source_ypath + "' has conflicting type with target node '" + node.target_ypath + "' ('" + node.source_attributes.type + "' -vs- '" + node.target_attributes.type + "'); aborting");
                    logger.close();
                    process.abort();
                }
                return cb();
            }
        });
    }, cb);
}

YtController.prototype._migrateNodes = function(concurrency, nodes, doMigrate, doVerify, cb) {
    var self = this;

    renderSubtitle("There are %d nodes in total", nodes.length);

    $.filter(nodes,
        function(node, cb) {
            if (typeof(node.target_attributes) !== "object") {
                logger.debug("Target node '" + node.target_ypath + "' does not exist; creating");
                return $.nextTick(_(cb).bind(null, true));
            }

            if (doVerify(node.source_attributes, node.target_attributes)) {
                logger.debug("Target node '" + node.target_ypath + "' is up-to-date; skipping");
                return $.nextTick(_(cb).bind(null, false));
            }

            logger.debug("Target node '" + node.target_ypath + "' requires update; dropping");
            self.target.execute("remove", node.target_ypath, function(err) {
                if (err) {
                    var error = util.format("Failed to remove target node '%s': %s", node.target_ypath, err.toString());
                    logger.error(error);
                    logger.close();
                    process.abort();
                }
                return cb(true);
            });
        },
        function(nodes) {
            renderSubtitle("There are %d nodes to be migrated", nodes.length);

            var semaphore = new Semaphore(nodes.length, nodes.length);

            $.forEachLimit(nodes, concurrency,
                function(node, cb_) {
                    var cb = function() { semaphore.decrement(); cb_.apply(null, arguments); };
                    return doMigrate(node, cb);
                },
                cb);
        });
}

YtController.prototype.migrateMapNodes = function(cb) {
    renderTitle("(3) About to migrate structural nodes");
    var self = this;

    self._migrateNodes(1,
        _(self.nodes).filter(function(node) {
            return node.source_attributes.type === "map_node"
        }),
        function doMigrate(node, cb) {
            self.target.execute("create", "map_node", node.target_ypath, function(err) {
                if (err) {
                    var error = "Failed to create target node '" + node.target_ypath + "'; aborting";
                    return cb(new Error(error));
                } else {
                    return cb();
                }
            });
        },
        function doVerify(expected_attributes, actual_attributes) {
            return true;
        },
        function() {
            logger.info("Finished migrating structural nodes");
            cb();
        });
}

YtController.prototype.migrateFiles = function(cb) {
    renderTitle("(4) About to migrate files");
    var self = this;

    self._migrateNodes(__CONCURRENCY,
        _(self.nodes).filter(function(node) {
            return node.source_attributes.type === "file"
        }),
        function doMigrate(node, cb) {
            source_process = self.source.spawn("download", node.source_ypath);
            target_process = self.target.spawn("upload", node.target_ypath);

            source_process.stdout.pipe(target_process.stdin);
            source_process.stdout.resume();

            var started_at = moment();

            var sp_exit_code = null;
            var tp_exit_code = null;

            var cb_if_needed = function cbIfNeeded() {
                if (sp_exit_code === null || tp_exit_code === null) {
                    return;
                }

                if (sp_exit_code !== 0) {
                    var error = util.format("Source ('download') process exited with non-zero exit code %d", sp_exit_code);
                    return cb(new Error(error));
                }

                if (tp_exit_code !== 0) {
                    var error = util.format("Target ('upload') process exited with non-zero exit code %d", tp_exit_code);
                    return cb(new Error(error));
                }

                logger.debug(util.format("Migrated file '%s' -> '%s' in %ss",
                    node.source_ypath,
                    node.target_ypath,
                    moment.duration(moment() - started_at).asSeconds()));

                $.parallel(
                    _(node.source_attributes).map(function(value, key) {
                        if (key in [ "executable", "file_name" ]) {
                            return $.apply(self.target.execute, "set", node.target_ypath + "/@" + key, JSON.stringify(value));
                        }
                    }),
                    cb);
            }

            source_process.on("exit", function(code) {
                sp_exit_code = code;
                cb_if_needed();
            });

            target_process.on("exit", function(code) {
                tp_exit_code = code;
                cb_if_needed();
            });
        },
        function doVerify(expected_attributes, actual_attributes) {
            return expected_attributes.size === actual_attributes.size;
        },
        function() {
            logger.info("Finished migrating files");
            cb();
        });
}

YtController.prototype.prepareToMigrateTables = function(cb) {
    renderTitle("(5) Uploading migration bundle");

    var self = this;
    var semaphore = new Semaphore(self.target.bundle.length, self.target.bundle.length);

    var uploader = function(file, cb_) {
        var cb = function() { semaphore.decrement(); cb_.apply(null, arguments); };

        var entry = {};
        entry.filename = fs.realpathSync(file);
        entry.basename = path.basename(entry.filename);
        entry.path = self.source.storage.concat(entry.basename);
        entry.ypath = ypathJoin(entry.path);

        var stream = fs.createReadStream(entry.filename);
        var child = self.source.spawn("upload", entry.ypath);

        stream.pipe(child.stdin);
        stream.resume();

        var started_at = moment();

        var stream_has_ended = false;
        var exit_code = null;

        var cb_if_needed = function cbIfNeeded() {
            if (!stream_has_ended || exit_code === null) {
                return;
            }

            if (exit_code !== 0) {
                var error = util.format("Process terminated with non-zero exit code %d", exit_code);
                return cb(new Error(error));
            }

            $.series([
                $.apply(self.source.execute, "set", entry.ypath + "/@executable", JSON.stringify("true")),
                $.apply(self.source.execute, "set", entry.ypath + "/@file_name", JSON.stringify(entry.basename))
            ], function(err) {
                if (err) {
                    return cb(err);
                } else {
                    return cb(null, entry);
                }
            });
        };

        stream.on("end", function() {
            stream_has_ended = true;
            cb_if_needed();
        });

        child.on("exit", function(code) {
            exit_code = code;
            cb_if_needed();
        });
    };

    $.series([
        $.apply(self.source.execute, "create", "map_node", ypathJoin(self.source.storage)),
        $.apply(self.source.execute, "create", "table", ypathJoin(self.source.storage.concat("__sink__"))),
        $.apply($.waterfall, [
            $.apply($.map, self.target.bundle, uploader),
            function(uploaded_bundle, cb) {
                self.target.uploaded_bundle = uploaded_bundle;
                cb();
            }])
    ], cb);
}

YtController.prototype.migrateTables = function(cb) {
    renderTitle("(5) About to migrate tables");
    var self = this;

    self._migrateNodes(1,
        _(self.nodes).filter(function(node) {
            return node.source_attributes.type === "table"
        }),
        function doMigrate(node, cb) {
            var cbs = [];

            cbs.push($.apply(self.target.execute, "create", "table", node.target_ypath));
            cbs.push($.apply(self.target.execute, "set", node.target_ypath + "/@channels", JSON.stringify(node.source_attributes.channels)));
            cbs.push(function(cb) {
                var invocation = _.chain([ "map" ])
                    .concat(_(self.target.uploaded_bundle).map(function(file) {
                        return [ "--file", file.ypath ];
                    }))
                    .concat("--in", ypathJoin(node.source_path))
                    .concat("--out", ypathJoin(self.source.storage.concat("__sink__")))
                    .concat("--command",
                        "LD_LIBRARY_PATH=`pwd` " +
                        "`pwd`/yt write " + 
                        "--config '" + path.basename(self.target.configuration) + "' " +
                        "'" + node.target_ypath + "' " +
                        "--config_opt /table_writer/upload_replication_factor=3"
                    )
                    .flatten()
                    .value();

                return self.source.communicate({
                    arguments: invocation,
                    ignore_stdout: true,
                    ignore_stderr: true,
                }, cb);
            });

            $.series(cbs, cb);
        },
        function doVerify(expected_attributes, actual_attributes) {
            logger.debug("While verifying expected row count is " + expected_attributes.row_count + ", while actual is " + actual_attributes.row_count);
            return expected_attributes.row_count === actual_attributes.row_count;
        },
        cb);
}

////////////////////////////////////////////////////////////////////////////////

var glob = require("glob");

var s = new Yt(
    "/home/sandello/archive/stable/yt",
    glob.sync("/home/sandello/archive/stable/lib*"),
    "/home/sandello/archive/ytdriver.test.conf");
var t = new Yt(
    "/home/sandello/archive/master/yt",
    glob.sync("/home/sandello/archive/master/lib*"),
    "/home/sandello/archive/ytdriver.migration.conf");
var m = new YtController(s, t);

async.series([
    m.gatherNodes.bind(m),
    m.examineNodes.bind(m),
    m.migrateMapNodes.bind(m),
    m.migrateFiles.bind(m),
    m.prepareToMigrateTables.bind(m),
    m.migrateTables.bind(m)
], function(err) {
    if (err) {
        console.log("whoop, i am dead: " + err.toString());
    } else {
        console.log("okay, i'm done here");
    }
});
