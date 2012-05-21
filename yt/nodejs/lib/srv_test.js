var connect = require("connect");
var app = require("./srv");
var fs = require("fs");

fs.readFile("yt-http-api.conf", function(err, configuration) {
    if (err) {
        throw err;
    }
    connect()
        .use(connect.favicon())
        .use(connect.logger("dev"))
        .use("/api", app.YtApplication(configuration.toString()))
        .listen(8000, "127.0.0.1");
});
