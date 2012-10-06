var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT(ALL|APP)/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Host Discovery:", x); };
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

function shuffle(array) {
    "use strict";

    var i = array.length;

    if (i === 0) {
        return false;
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

        // TODO: Use proper acceptsType() implementation which respects order and quality.
        if (typeof(accept) === "string") {
            /****/ if (utils.acceptsType("application/json", accept)) {
                body = JSON.stringify(body);
                rsp.writeHead(200, {
                    "Content-Length" : body.length,
                    "Content-Type" : "application/json"
                });
            } else if (utils.acceptsType("text/plain", accept)) {
                body = body.toString("\n");
                rsp.writeHead(200, {
                    "Content-Length" : body.length,
                    "Content-Type" : "text/plain"
                });
            } else {
                // TODO: Emit 406 or 416 here.
                // Unsupported
                (function(){} ());
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
