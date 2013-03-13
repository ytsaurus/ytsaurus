var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("H", "Host Discovery");

////////////////////////////////////////////////////////////////////////////////

function shuffle(array) {
    "use strict";

    var i = array.length;

    if (i === 0) {
        return [];
    }

    while (--i) {
        var j   = Math.floor(Math.random() * (i + 1));
        var lhs = array[i];
        var rhs = array[j];
        array[i] = rhs;
        array[j] = lhs;
    }

    return array;
}

////////////////////////////////////////////////////////////////////////////////

exports.that = function YtHostDiscovery(hosts) {
    "use strict";

    __DBG("New with the following hosts: " + JSON.stringify(hosts, null, 2));

    return function(req, rsp) {
        var body = shuffle(hosts);
        var accept = req.headers["accept"];
        var accepted_type;

        if (req.headers["host"] === "proxy-fb.yt.yandex.net") {
            for (var i = 0; i < body.length; ++i) {
                body[i] = body[i].replace(/\.yt\.yandex\.net/, '-fb.yt.yandex.net');
            }
        }

        rsp.setHeader("Access-Control-Allow-Origin", "*");

        if (typeof(accept) === "string") {
            accepted_type = utils.bestAcceptedType(
                [ "application/json", "text/plain" ],
                accept);

            if (accepted_type === "application/json") {
                body = JSON.stringify(body);
                rsp.writeHead(200, {
                    "Content-Length" : body.length,
                    "Content-Type" : "application/json"
                });
            } else if (accepted_type === "text/plain") {
                body = body.join("\n");
                rsp.writeHead(200, {
                    "Content-Length" : body.length,
                    "Content-Type" : "text/plain"
                });
            } else {
                rsp.statusCode = 406;
                rsp.end();
            }
        } else {
            body = JSON.stringify(body);
            rsp.writeHead(200, {
                "Content-Length" : body.length,
                "Content-Type" : "application/json"
            });
        }

        rsp.end(body);
    };
};
