var connect = require("connect");
var app     = require("./srv");

connect()
    .use(connect.favicon())
    .use(connect.logger("dev"))
    .use("/api", app.YtApplication())
    .listen(8000, "127.0.0.1");