exports.TraceEvent = function(object, sname, ename) {
    "use strict";
    object.on(ename, function() { console.log("---> %s : %s", sname, ename); });
};

exports.TraceReadableStream = function(stream, name) {
    "use strict";
    exports.TraceEvent(stream, name, "data");
    exports.TraceEvent(stream, name, "end");
    exports.TraceEvent(stream, name, "error");
    exports.TraceEvent(stream, name, "close");
};

exports.TraceWritableStream = function(stream, name) {
    "use strict";
    exports.TraceEvent(stream, name, "drain");
    exports.TraceEvent(stream, name, "error");
    exports.TraceEvent(stream, name, "close");
    exports.TraceEvent(stream, name, "pipe");
};

exports.TraceSocket = function(socket, name) {
    "use strict";
    exports.TraceEvent(socket, name, "connect");
    exports.TraceEvent(socket, name, "data");
    exports.TraceEvent(socket, name, "end");
    exports.TraceEvent(socket, name, "timeout");
    exports.TraceEvent(socket, name, "drain");
    exports.TraceEvent(socket, name, "error");
    exports.TraceEvent(socket, name, "close");
};

exports.TraceSocketActivity = function(socket, name) {
    "use strict";
    setTimeout(function inner() {
        console.log("--? %s : Peer=%s:%s Recv=%s Send=%s Buffer=%s",
            name,
            socket.remoteAddress, socket.remotePort,
            socket.bytesRead, socket.bytesWritten,
            socket.bufferSize);
        setTimeout(inner, 1000);
    }, 1000);
};

exports.that = function(key, name) {
    "use strict";
    var test = new RegExp("YT(" + [ "ALL", key ].join("|") + ")");
    var result;

    if (process.env.NODE_DEBUG && test.test(process.env.NODE_DEBUG)) {
        result = function(x) {
            "use strict";
            console.error("YT: " + name + ":", x);
        };
        result.UUID = require("node-uuid");
    } else {
        result = function(){};
    }

    return result;
};
