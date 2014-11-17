/*jshint forin: false*/

////////////////////////////////////////////////////////////////////////////////
// These functions are mainly from Express framework.
// http://expressjs.com/
////////////////////////////////////////////////////////////////////////////////

var util = require("util");
var stream = require("stream");

// Redirects unless original URL is not a directory.
exports.redirectUnlessDirectory = function(req, rsp)
{
    "use strict";
    if (req.url === "/" &&
        req.originalUrl && req.originalUrl.substr(-1) !== "/"
    ) {
        exports.redirectTo(rsp, req.originalUrl + "/", 301);
        return true;
    } else {
        return false;
    }
};

// Redirects request to a predefined location.
exports.redirectTo = function(rsp, target, code)
{
    "use strict";
    rsp.removeHeader("Vary");
    rsp.removeHeader("Transfer-Encoding");
    rsp.removeHeader("Content-Encoding");
    rsp.removeHeader("Content-Disposition");
    rsp.removeHeader("Content-Type");
    rsp.setHeader("Location", target);
    rsp.setHeader("Content-Length", 0);
    rsp.setHeader("Connection", "close");
    rsp.shouldKeepAlive = false;
    rsp.writeHead(code || 303);
    rsp.end();
};

// Dispatches request with a precomputed result.
exports.dispatchAs = function(rsp, body, type)
{
    "use strict";
    rsp.removeHeader("Vary");
    rsp.removeHeader("Transfer-Encoding");
    rsp.removeHeader("Content-Encoding");
    rsp.removeHeader("Content-Disposition");
    if (typeof(type) === "string") {
        rsp.setHeader("Content-Type", type);
    } else {
        rsp.removeHeader("Content-Type");
    }
    if (typeof(body) !== "undefined") {
        rsp.setHeader("Content-Length",
            typeof(body) === "string" ?
            Buffer.byteLength(body) :
            body.length);
    } else {
        rsp.setHeader("Content-Length", 0);
    }
    rsp.setHeader("Connection", "close");
    rsp.shouldKeepAlive = false;
    rsp.writeHead(rsp.statusCode || 200);
    rsp.end(body);
};

exports.dispatchJson = function(rsp, object)
{
    "use strict";
    exports.dispatchAs(rsp, JSON.stringify(object), "application/json");
};

// Dispatches a 401.
exports.dispatchUnauthorized = function(rsp, scope)
{
    "use strict";
    rsp.statusCode = 401;
    rsp.setHeader("WWW-Authenticate", scope);
    exports.dispatchAs(rsp);
};

// Dispatches a 503.
exports.dispatchLater = function(rsp, after)
{
    "use strict";
    rsp.statusCode = 503;
    rsp.setHeader("Retry-After", after);
    exports.dispatchAs(rsp);
};

// Checks whether MIME pattern |mime| matches actual MIME type |actual|.
exports.matches = function(mime, actual)
{
    "use strict";
    if (!actual) {
        return false;
    }

    actual = actual.split(";")[0];

    if (mime.indexOf("*") !== -1) {
        mime = mime.split("/");
        actual = actual.split("/");

        if (mime.length !== 2 || actual.length !== 2) {
            return false;
        }

        return (mime[0] === "*" && mime[1] === actual[1]) ||
               (mime[1] === "*" && mime[0] === actual[0]) ||
               (mime[0] === "*" && mime[1] === "*");
    } else {
        return mime === actual;
    }
};

// Returns best MIME type in |mimes| which is accepted by Accept header |header|.
exports.bestAcceptedType = function(mimes, header)
{
    "use strict";
    if (!header) {
        return mimes[0];
    }

    var accepted = exports.parseAcceptType(header);
    var mime, parts;

    for (var i = 0, imax = accepted.length; i < imax; ++i) {
        for (var j in mimes) {
            mime = mimes[j];
            parts = mime.split("/");
            if ((accepted[i].type    === parts[0] || accepted[i].type    === "*") &&
                (accepted[i].subtype === parts[1] || accepted[i].subtype === "*"))
            {
                return mime;
            }
        }
    }

    return undefined;
};

// Returns best encoding in |encodings| which is accepted by Accept-Encoding header |header|.
exports.bestAcceptedEncoding = function(encodings, header)
{
    "use strict";
    if (!header) {
        return encodings[0];
    }

    var accepted = exports.parseAcceptEncoding(header);
    var encoding;

    for (var i = 0, imax = accepted.length; i < imax; ++i) {
        for (var j in encodings) {
            encoding = encodings[j];
            if (accepted[i].value === encoding || accepted[i].value === "*") {
                return encoding;
            }
        }
    }

    return undefined;
};

// Auxiliary.
exports.parseAcceptType = function(header)
{
    "use strict";
    return header
        .split(/ *, */)
        .map(exports.parseQuality)
        .filter(function(x) {
            return x.quality;
        })
        .sort(function(a, b) {
            return b.quality - a.quality;
        })
        .map(function(x) {
            var parts = x.value.split("/");
            x.type    = parts[0];
            x.subtype = parts[1];
            return x;
        });
};

// Auxiliary.
exports.parseAcceptEncoding = function(header)
{
    "use strict";
    return header
        .split(/ *, */)
        .map(exports.parseQuality)
        .filter(function(x) {
            return x.quality;
        })
        .sort(function(a, b) {
            return b.quality - a.quality;
        });
};

// Auxiliary.
exports.parseQuality = function(header)
{
    "use strict";

    var parts = header.split(/ *; */);
    var value = parts[0];

    var quality = parts[1] ? parseFloat(parts[1].split(/ *= */)[1]) : 1.0;

    return { value : value, quality : quality };
};

/**
 * Recursively traverses non-cyclic structure and replaces everything
 * that looks like a number with a number.
 */
exports.numerify = function(obj)
{
    "use strict";
    if (typeof(obj) === "object") {
        for (var key in obj) {
            if (obj.hasOwnProperty(key)) {
                obj[key] = exports.numerify(obj[key]);
            }
        }
    } else if (typeof(obj) === "array") {
        for (var index in obj) {
            obj[index] = exports.numerify(obj[index]);
        }
    } else if (typeof(obj) === "string") {
        var objNum = +obj;
        if (obj === objNum.toString(10)) {
            obj = objNum;
        }
    }
    return obj;
};

/**
 * Recursively merge properties of two objects.
 * Preference is given to right-hand side.
 */
exports.merge = function (lhs, rhs)
{
    "use strict";
    for (var p in rhs) {
        try {
            if (typeof(rhs[p]) !== "undefined") {
                if (rhs[p].constructor === Object) {
                    lhs[p] = exports.merge(lhs[p], rhs[p]);
                } else {
                    lhs[p] = rhs[p];
                }
            }
        } catch (err) {
            lhs[p] = rhs[p];
        }
    }
    return lhs;
};

////////////////////////////////////////////////////////////////////////////////

exports.NullStream = function()
{
    "use strict";
    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    var self = this;
    process.nextTick(function() {
        self.emit("end");
        self.readable = false;
    });
};

util.inherits(exports.NullStream, stream.Stream);

exports.NullStream.prototype.pause = function(){};
exports.NullStream.prototype.resume = function(){};
exports.NullStream.prototype.destroy = function(){};

////////////////////////////////////////////////////////////////////////////////

exports.Pause = function(slave)
{
    "use strict";
    var on_data, on_end, events = [];
    var dummy = function() {};

    slave.on("data", on_data = function(data, encoding) {
        events.push(["data", data, encoding]);
    });

    slave.on("end", on_end = function(data, encoding) {
        events.push(["end", data, encoding]);
    });

    slave.pause();

    return {
        dispose: function() {
            slave.removeListener("data", on_data);
            slave.removeListener("end", on_end);
        },
        unpause: function() {
            this.dispose();
            for (var i = 0, n = events.length; i < n; ++i) {
                slave.emit.apply(slave, events[i]);
            }
            slave.resume();

            this.dispose = dummy;
            this.unpause = dummy;
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

var TAGGED_LOGGER_LEVELS = [ "info", "warn", "debug", "error" ];

exports.TaggedLogger = function(logger, delta)
{
    "use strict";

    var self = this;

    TAGGED_LOGGER_LEVELS.forEach(function(level) {
        var func = logger[level];
        self[level] = function(message, payload) {
            payload = payload || {};
            for (var p in delta) {
                if (delta.hasOwnProperty(p)) {
                    if (typeof(delta[p]) === "function") {
                        payload[p] = delta[p]();
                    } else {
                        payload[p] = delta[p];
                    }
                }
            }
            return func.call(logger, message, payload);
        };
    });

    return this;
};

////////////////////////////////////////////////////////////////////////////////

exports.MemoryInputStream = function(data)
{
    "use strict";
    stream.Stream.call(this);

    this.paused = false;
    this.readable = true;
    this.writable = false;

    var self = this;

    var emitted_data = false;
    var emit_data = function() {
        if (emitted_data || self.paused || !self.readable) {
            return;
        }
        if (data) {
            self.emit("data", data);
        }
        emitted_data = true;
    };

    var emitted_end = false;
    var emit_end = function() {
        if (emitted_end || self.paused || !self.readable) {
            return;
        }
        self.emit("end", data);
        emitted_end = true;
        // Block state.
        self.paused = false;
        self.readable = false;
        self.writable = false;
    };

    this._flow = function() {
        process.nextTick(function() {
            emit_data();
            process.nextTick(function() {
                emit_end();
            });
        });
    };
};

util.inherits(exports.MemoryInputStream, stream.Stream);

exports.MemoryInputStream.prototype.pause = function()
{
    "use strict";
    this.paused = true;
};

exports.MemoryInputStream.prototype.resume = function()
{
    "use strict";
    this.paused = false;
    this._flow();
};

exports.MemoryInputStream.prototype.destroy = function()
{
    "use strict";
    this.readable = false;
};

////////////////////////////////////////////////////////////////////////////////

exports.MemoryOutputStream = function()
{
    "use strict";
    stream.Stream.call(this);

    this.readable = false;
    this.writable = true;

    this.chunks = [];
};

util.inherits(exports.MemoryOutputStream, stream.Stream);

exports.MemoryOutputStream.prototype.write = function(chunk)
{
    "use strict";
    if (chunk) {
        this.chunks.push(chunk);
    }
    return true;
};

exports.MemoryOutputStream.prototype.end = function(chunk)
{
    "use strict";
    if (chunk) {
        this.chunks.push(chunk);
    }
    this.writable = false;
};

exports.MemoryOutputStream.prototype.destroy = function(){};

////////////////////////////////////////////////////////////////////////////////

exports.getYsonValue = function(x)
{
    "use strict";
    if (typeof(x) === "object" && typeof(x.$value) !== "undefined") {
        return x.$value;
    } else {
        return x;
    }
};

exports.getYsonAttribute = function(x, attribute)
{
    "use strict";
    if (typeof(x) === "object" && typeof(x.$attributes) !== "undefined") {
        return x.$attributes[attribute];
    }
};

exports.escapeYPath = function(s)
{
    "use strict";
    return s.replace(/([\/@&])/g, '\\$1');
};

exports.escapeHeader = function(x)
{
    "use strict";
    return String(x)
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
};
