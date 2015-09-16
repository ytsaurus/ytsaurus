var http = require("http");
var connect = require("connect");

exports.srv = function srv()
{
    var app = connect()
        .use(function(req, rsp, next) {
            req.uuid = require("crypto").pseudoRandomBytes(8).toString("hex");
            next();
        });

    var middleware = Array.prototype.slice.call(arguments);
    var done = middleware.pop();

    middleware.forEach(function(step) { app.use(step); });
    app.use(function(req, rsp) { rsp.end("Rabbit Hole"); });

    var server = http.createServer(app);

    server.listen(0, HTTP_HOST, function() {
        var x = server.address();
        HTTP_HOST = x.address;
        HTTP_PORT = x.port;
        done();
    });

    return server;
};

exports.die = function die(server, done)
{
    server.close(done);
};

exports.ask = function ask(method, path, headers, verify, callback)
{
    var options = connect.utils.merge({
        method : method,
        host : HTTP_HOST,
        port : HTTP_PORT,
        path : path,
        headers : headers
    });

    return http.request(options, function(rsp) {
        var body = "";

        rsp.on("data", function(chunk) { body += chunk.toString(); });
        rsp.on("end",  function() {
            try {
                if (body.length) {
                    try {
                        var bodyParsed = JSON.parse(body);
                        rsp.json = bodyParsed;

                        // TODO(sandello): Rework.
                        if (rsp.headers["x-yt-error"]) {
                            if (bodyParsed.hasOwnProperty("error")) {
                                __DBG("\n*** HTTP Response Error:\n" + bodyParsed.error);
                                delete bodyParsed.error;
                            }
                            if (bodyParsed.hasOwnProperty("error_trace")) {
                                __DBG("\n*** HTTP Response Error Stack:\n" + bodyParsed.error_trace);
                                delete bodyParsed.error_trace;
                            }
                        }

                        var bodyFormatted = JSON.stringify(bodyParsed, null, 2);
                        __DBG("\n*** HTTP Response Body:\n" + bodyFormatted + "\n***");
                    } catch (err) {
                        __DBG("\n*** HTTP Response Body:\n" + body + "\n***");
                    }
                }

                rsp.body = body;

                verify(rsp);
                callback();
            } catch (err) {
                callback(err);
            }
        });
    });
};
