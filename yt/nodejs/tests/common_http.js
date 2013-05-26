var http = require("http");
var connect = require("connect");

exports.srv = function srv()
{
    ++HTTP_PORT; // Increment port to avoid EADDRINUSE failures.

    var server = connect()
        .use(function(req, rsp, next) {
            req.uuid = require("node-uuid").v4();
            next();
        });

    var middleware = Array.prototype.slice.call(arguments);
    var done = middleware.pop();
    var doneRight = function() { setTimeout(done, 1); };

    middleware.forEach(function(step) { server.use(step); });

    return server
        .use(function(req, rsp) { rsp.end("Rabbit Hole"); })
        .listen(HTTP_PORT, HTTP_HOST, doneRight);
};

exports.die = function die(server, done)
{
    server.close(function() { setTimeout(done, 1); });
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
                                __DBG("*** HTTP Response Error:\n" + bodyParsed.error);
                                delete bodyParsed.error;
                            }
                            if (bodyParsed.hasOwnProperty("error_trace")) {
                                __DBG("*** HTTP Response Error Stack:\n" + bodyParsed.error_trace);
                                delete bodyParsed.error_trace;
                            }
                        }

                        var bodyFormatted = JSON.stringify(bodyParsed, null, 2);
                        __DBG("*** HTTP Response Body:\n" + bodyFormatted + "\n***");
                    } catch(err) {
                        __DBG("*** HTTP Response Body:\n" + body + "\n***");
                    }
                }

                rsp.body = body;

                verify(rsp);
                callback();
            } catch(err) {
                callback(err);
            }
        });
    });
};
