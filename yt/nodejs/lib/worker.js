// Dependencies.
var cluster = require("cluster");
var fs = require("fs");
var http = require("http");
var https = require("https");

var yt = require("yt");

var connect = require("connect");
var node_static = require("node-static");
var winston = require("winston");

var Q = require("q");

var profiler = require("profiler");
var heapdump = require("heapdump");

// Debugging stuff.
var __DBG = require("./debug").that("C", "Cluster Worker");
var __PROFILE = false;

// Load configuration.
var config = JSON.parse(process.env.YT_PROXY_CONFIGURATION);

// Set up logging (the hard way).
var logger_mediate = function(level, message, payload) {
    // Capture real message timestamp before sending an event.
    payload = payload || {};
    payload["timestamp"] = new Date().toISOString();

    process.send({
        type: "log",
        level: level,
        message: message,
        payload: payload
    });
};

var logger = {
    debug: function(message, payload) {
        return logger_mediate("debug", message, payload);
    },
    info: function(message, payload) {
        return logger_mediate("info", message, payload);
    },
    warn: function(message, payload) {
        return logger_mediate("warn", message, payload);
    },
    error: function(message, payload) {
        return logger_mediate("error", message, payload);
    },
};

var version;

// Speed stuff.
Q.longStackJumpLimit = 0;

try {
    version = JSON.parse(fs.readFileSync(__dirname + "/../package.json"));
} catch (ex) {
    version = { version : "(development)", versionFull: "(development)", dependencies : {} };
}

// TODO(sandello): Extract these settings somewhere.
if (typeof(config.low_watermark) === "undefined") {
    config.low_watermark = parseInt(0.80 * config.memory_limit, 10);
}

if (typeof(config.high_watermark) === "undefined") {
    config.high_watermark = parseInt(0.95 * config.memory_limit, 10);
}

// TODO(sandello): Extract singleton configuration to a separate branch.
yt.configureSingletons(config.proxy);

yt.YtRegistry.set("fqdn", config.fqdn || require("os").hostname());
yt.YtRegistry.set("config", config);
yt.YtRegistry.set("logger", logger);
yt.YtRegistry.set("driver", new yt.YtDriver(config));
yt.YtRegistry.set("authority", new yt.YtAuthority(
    config.authentication,
    yt.YtRegistry.get("driver")));
yt.YtRegistry.set("coordinator", new yt.YtCoordinator(
    config.coordination,
    new yt.utils.TaggedLogger(logger, { wid: cluster.worker.id, pid: process.pid }),
    yt.YtRegistry.get("driver"),
    yt.YtRegistry.get("fqdn")));

// Hoist variable declaration.
var application;

var insecure_server;
var secure_server;

var insecure_listening_deferred = Q.defer();
var secure_listening_deferred = Q.defer();

var insecure_close_deferred = Q.defer();
var secure_close_deferred = Q.defer();

var violentlyDieTriggered = false;
var violentlyDie = function violentDeath() {
    "use strict";
    if (violentlyDieTriggered) { return; }
    violentlyDieTriggered = true;

    process.nextTick(function() {
        cluster.worker.disconnect();
        cluster.worker.destroy();
        process.exit();
    });
};

var gracefullyDieTriggered = false;
var gracefullyDie = function gracefulDeath() {
    "use strict";
    if (gracefullyDieTriggered) { return; }
    gracefullyDieTriggered = true;

    logger.info("Prepairing to die", { wid : cluster.worker.id, pid : process.pid });
    process.send({ type : "stopping" });

    try {
        !!insecure_server ? insecure_server.close() : secure_close_deferred.resolve();
    } catch (ex) {
        logger.error("Caught exception during HTTP shutdown: " + ex.toString());
    }
    try {
        !!secure_server ? secure_server.close() : secure_close_deferred.resolve();
    } catch (ex) {
        logger.error("Caught exception during HTTP shutdown: " + ex.toString());
    }
};

// Fire up the heart.
var supervisor_liveness;

if (!__DBG.On) {
    (function sendHeartbeat() {
        "use strict";
        process.send({ type : "heartbeat" });
        setTimeout(sendHeartbeat, 2000);
    }());

    supervisor_liveness = setTimeout(gracefullyDie, 30000);
}

// Setup signal handlers.
process.on("SIGUSR2", function() {
    "use strict";
    console.error("Writing a heap snapshot (" + process.pid + ")");
    heapdump.writeSnapshot();
});

process.on("SIGUSR1", function() {
    "use strict";
    if (__PROFILE) {
        console.error("Pausing V8 profiler.");
        profiler.pause();
    } else {
        console.error("Resuming V8 profiler.");
        profiler.resume();
    }
    __PROFILE = !__PROFILE;
});

// Setup message handlers.
process.on("message", function(message) {
    "use strict";
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

application = connect()
    .use(yt.YtIsolateRequest())
    .use(yt.YtLogRequest())
    .use(yt.YtAcao())
    .use(yt.YtMinPythonWrapperVersion())
    .use(connect.favicon())
    .use("/hosts", yt.YtApplicationHosts())
    .use("/auth", yt.YtApplicationAuth())
    .use("/upravlyator", yt.YtApplicationUpravlyator())
    // TODO(sandello): Can we remove this?
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
    // TODO(sandello): This would be deprecated with nodejs 0.10.
    // Begin of asynchronous middleware.
    .use(function(req, rsp, next) {
        "use strict";
        req.pauser = yt.Pause(req);
        next();
    })
    .use(yt.YtAuthentication())
    .use("/api", yt.YtApplicationApi())
    .use(function(req, rsp, next) {
        "use strict";
        process.nextTick(function() { req.pauser.unpause(); });
        next();
    })
    // End of asynchronous middleware.
    .use("/ping", function(req, rsp, next) {
        "use strict";
        if (req.url === "/") {
            return void yt.utils.dispatchAs(rsp, "");
        }
        next();
    })
    .use("/version", function(req, rsp, next) {
        "use strict";
        if (req.url === "/") {
            return void yt.utils.dispatchAs(rsp, version.versionFull, "text/plain");
        }
        next();
    });

config.redirect.forEach(function(site) {
    var web_path = site[0].replace(/\/+$/, "");
    var real_path = site[1].replace(/\/+$/, "");
    application.use(web_path, function(req, rsp, next) {
        "use strict";
        return void yt.utils.redirectTo(rsp, real_path + req.url, 301);
    });
});

config.static.forEach(function(site) {
    var web_path = site[0].replace(/\/+$/, "");
    var real_path = site[1];
    var server = new node_static.Server(real_path, {
        cache: 3600,
        gzip: true,
    });
    application.use(web_path, function(req, rsp, next) {
        "use strict";
        if (req.url === "/") {
            if (req.originalUrl === web_path) {
                return void yt.utils.redirectTo(rsp, web_path + "/");
            }
            req.url = "index.html";
        }
        req.on("end", function() {
            server.serve(req, rsp);
        });
    });
});

application
    .use("/", function(req, rsp, next) {
        "use strict";
        if (req.url === "/") {
            return void yt.utils.redirectTo(rsp, "/ui/");
        }
        next();
    })
    .use(function(req, rsp) {
        "use strict";
        rsp.statusCode = 404;
        return void yt.utils.dispatchAs(
            rsp, "Invalid URI " + JSON.stringify(req.url) + ". " +
            "Please refer to documentation at http://wiki.yandex-team.ru/YT/ " +
            "to learn more about HTTP API.",
            "text/plain");
    });

// Bind servers.
if (config.port && config.address) {
    logger.info("Binding to insecure socket", {
        wid: cluster.worker.id,
        pid: process.pid,
        address: config.address,
        port: config.port
    });

    insecure_server = http.createServer(application);
    insecure_server.listen(config.port, config.address);

    insecure_server.on("close", insecure_close_deferred.resolve.bind(insecure_close_deferred));
    insecure_server.on("listening", insecure_listening_deferred.resolve.bind(insecure_listening_deferred));
    insecure_server.on("connection", yt.YtLogSocket());
    insecure_server.on("connection", function(socket) {
        "use strict";
        socket.setTimeout(5 * 60 * 1000);
        socket.setNoDelay(true);
        socket.setKeepAlive(true);
    });
} else {
    insecure_close_deferred.reject(new Error("Insecure server is not enabled"));
    insecure_listening_deferred.reject(new Error("Insecure server is not enabled"));
}

if (config.ssl_port && config.ssl_address) {
    logger.info("Binding to secure socket", {
        wid: cluster.worker.id,
        pid: process.pid,
        address: config.ssl_address,
        port: config.ssl_port
    });

    var ssl_key = fs.readFileSync(config.ssl_key);
    var ssl_certificate = fs.readFileSync(config.ssl_certificate);
    var ssl_ca = null;
    if (config.ssl_ca) {
        ssl_ca = fs.readFileSync(config.ssl_ca);
    }

    secure_server = https.createServer({
        key: ssl_key,
        passphrase: config.ssl_passphrase,
        cert: ssl_certificate,
        ca: ssl_ca,
        ciphers: config.ssl_ciphers,
        rejectUnauthorized: config.ssl_reject_unauthorized,
    }, application);
    secure_server.listen(config.ssl_port, config.ssl_address);

    secure_server.on("close", secure_close_deferred.resolve.bind(secure_close_deferred));
    secure_server.on("listening", secure_listening_deferred.resolve.bind(secure_listening_deferred));
    secure_server.on("secureConnection", yt.YtLogSocket());
} else {
    secure_close_deferred.reject(new Error("Secure server is not enabled"));
    secure_listening_deferred.reject(new Error("Secure server is not enabled"));
}

// TODO(sandello): Add those checks in |secureConnection|.
// $ssl_client_i_dn == "/DC=ru/DC=yandex/DC=ld/CN=YandexExternalCA";
// $ssl_client_s_dn == "/C=RU/ST=Russia/L=Moscow/O=Yandex/OU=Information Security/CN=agranat.yandex-team.ru/emailAddress=security@yandex-team.ru";

Q
    .allResolved([ insecure_listening_deferred.promise, secure_listening_deferred.promise ])
    .then(function() {
        "use strict";
        logger.info("Worker is up and running", {
            wid : cluster.worker.id,
            pid : process.pid
        });
        process.send({ type : "alive" });
    });

Q
    .allResolved([ insecure_close_deferred.promise, secure_close_deferred.promise ])
    .then(violentlyDie);
