var connect = require("connect");
var app = require("./srv");
var fs = require("fs");

fs.readFile("yt-http-api.conf", function(configuration) {
    connect()
        .use(connect.favicon())
        .use(connect.logger("dev"))
        .use("/api", app.YtApplication(configuration))
        .listen(8000, "127.0.0.1");
});