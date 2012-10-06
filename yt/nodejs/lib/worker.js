// Dependencies.
var fs = require("fs");

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
        })
    ]
});

var version;

try {
    version = JSON.parse(fs.readFileSync(__dirname + "/../package.json"));
} catch (ex) {
    version = { version : "(development)", dependencies : {} };
}

// Fire up.
logger.info("Starting HTTP proxy worker", { pid : process.pid });
console.error("Starting worker (PID: %s)", process.pid);

// Yield application server.
var static_server = new node_static.Server(config.user_interface, { cache : 4 * 3600 });

module.exports = connect()
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
    .use("/__config__", function(req, rsp) {
        "use strict";
        req.on("end", function() { rsp.end(JSON.stringify(config)); });
    })
    .use("/__env__", function(req, rsp) {
        "use strict";
        req.on("end", function() { rsp.end(JSON.stringify(process.env)); });
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
    });
