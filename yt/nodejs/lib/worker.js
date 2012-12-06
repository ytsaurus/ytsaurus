// Dependencies.
var fs = require("fs");
var cluster = require("cluster");

var connect = require("connect");
var node_static = require("node-static");
var winston = require("winston");
var winston_nssocket = require("winston-nssocket");

var yt = require("yt");

// Load configuration and set up logging.
var config = JSON.parse(process.env.YT_PROXY_CONFIGURATION);
var logger = new winston.Logger({
    transports: [
        new winston_nssocket.Nssocket({
            host : config.log_address,
            port : config.log_port
        }),
        new winston.transports.Console({
            level : "info",
            timestamp : true
        })
    ]
});

var version;

try {
    version = JSON.parse(fs.readFileSync(__dirname + "/../package.json"));
} catch (ex) {
    version = { version : "(development)", versionFull: "(development)", dependencies : {} };
}

// Hoist variable declaration.
var static_server;
var dynamic_server;

var violentlyDieTriggered = false;
var violentlyDie = function violentDeath() {
    if (violentlyDieTriggered) { return; }
    violentlyDieTriggered = true;

    logger.info("Dying", { wid : cluster.worker.id, pid : process.pid });
    process.send({ type: "stopped" });

    process.nextTick(function() {
        cluster.worker.disconnect();
        cluster.worker.destroy();
        process.exit();
    });
};

var gracefullyDieTriggered = false;
var gracefullyDie = function gracefulDeath() {
    if (gracefullyDieTriggered) { return; }
    gracefullyDieTriggered = true;

    logger.info("Prepairing to die", { wid : cluster.worker.id, pid : process.pid });
    process.send({ type : "stopping" });

    if (dynamic_server && dynamic_server.close) {
        dynamic_server.close(violentlyDie);
    } else {
        violentlyDie();
    }
};

// Fire up the heart.
(function sendHeartbeat() {
    process.send({ type : "heartbeat" });
    setTimeout(sendHeartbeat, 1000);
})();

var supervisor_liveness = setTimeout(gracefullyDie, 15000);

// Setup message handlers.
process.on("message", function(message) {
    if (!message || !message.type) {
        return; // Improper message format.
    }

    switch (message.type) {
        case "heartbeat":
            if (supervisor_liveness) {
                clearTimeout(supervisor_liveness);
            }
            supervisor_liveness = setTimeout(gracefullyDie, 15000);
            break;
        case "gracefullyDie":
            gracefullyDie();
            break;
        case "violentlyDie":
            violentlyDie();
            break;
    }
});

// Fire up the head.
logger.info("Starting HTTP proxy worker", { wid : cluster.worker.id, pid : process.pid });

// Setup application server.
static_server = new node_static.Server(config.user_interface, { cache : 4 * 3600 });
dynamic_server = connect()
    .use(connect.favicon())
    .use(yt.YtAssignRequestId())
    .use(yt.YtLogRequest(logger))
    .use("/api", yt.YtApplication(logger, config))
    .use("/hosts", yt.YtHostDiscovery(config.neighbours))
    .use("/ui", function(req, rsp, next) {
        "use strict";
        if (req.url === "/") {
            req.url = "index.html";
        }
        req.on("end", function() {
            static_server.serve(req, rsp);
        });
    })
    .use("/_check_availability_time", function(req, rsp, next) {
        "use strict";
        fs.readFile("/var/lock/yt_check_availability_time", function(err, data) {
            if (err) {
                var body = "0";
                rsp.writeHead(200, { "Content-Length" : body.length, "Content-Type" : "text/plain" });
                rsp.end(body);
            } else {
                rsp.writeHead(200, { "Content-Length" : data.length, "Content-Type" : "text/plain" });
                rsp.end(data);
            }
        });
    })
    .use("/__version__", function(req, rsp) {
        "use strict";
        req.on("end", function() {
            rsp.setHeader("Content-Type", "text/plain");
            rsp.end(version.versionFull);
        });
    })
    .use("/__config__", function(req, rsp) {
        "use strict";
        req.on("end", function() {
            rsp.setHeader("Content-Type", "application/json");
            rsp.end(JSON.stringify(config));
        });
    })
    .use("/__env__", function(req, rsp) {
        "use strict";
        req.on("end", function() {
            rsp.setHeader("Content-Type", "application/json");
            rsp.end(JSON.stringify(process.env));
        });
    })
    .use("/", function(req, rsp, next) {
        "use strict";
        if (req.url === "/") {
            rsp.writeHead(303, {
                "Location" : "/ui/",
                "Content-Length" : 0,
                "Content-Type" : "application/json"
            });
            rsp.end();
        } else {
            next();
        }
    })
    .use(function(req, rsp) {
        "use strict";
        rsp.writeHead(404, { "Content-Type" : "text/plain" });
        rsp.end("Invalid URI " + JSON.stringify(req.url) + ". Please refer to documentation at http://wiki.yandex-team.ru/YT/ to learn more about HTTP API.");
    })
    .listen(config.port, config.address, function() {
        logger.info("Worker is listening", {
            wid : cluster.worker.id,
            pid : process.pid
        });
        process.send({ type : "alive" });
    })
    .on("close", violentlyDie);

