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
    var sent_headers = !!rsp._header;
    if (!sent_headers) {
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
    }
    rsp.end();
};

// Dispatches request with a precomputed result.
exports.dispatchAs = function(rsp, body, type)
{
    var sent_headers = !!rsp._header;
    if (!sent_headers) {
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
    }
    rsp.writeHead(rsp.statusCode || 200);
    rsp.end(body);
};

exports.dispatchJson = function(rsp, object)
{
    exports.dispatchAs(rsp, JSON.stringify(object), "application/json");
};

// Dispatches a 401.
exports.dispatchUnauthorized = function(rsp, scope, message)
{
    rsp.statusCode = 401;
    rsp.setHeader("WWW-Authenticate", scope);
    // Mimic to YtError.
    exports.dispatchJson(rsp, {code: 1, message: message, attributes: {}, inner_errors: []});
};

// Dispatches a 503.
exports.dispatchLater = function(rsp, after, message)
{
    rsp.statusCode = 503;
    rsp.setHeader("Retry-After", after);
    // Mimic to YtError.
    exports.dispatchJson(rsp, {code: 1, message: message, attributes: {}, inner_errors: []});
};

// Checks whether MIME pattern |mime| matches actual MIME type |actual|.
exports.matches = function(mime, actual)
{
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
    if (typeof(obj) === "object") {
        for (var key in obj) {
            if (obj.hasOwnProperty(key)) {
                obj[key] = exports.numerify(obj[key]);
            }
        }
    } else if (Array.isArray(obj)) {
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
exports.merge = function(lhs, rhs)
{
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

/**
 * Adds leading symbols to input
 * @param {String} string input
 * @param {String} pad leading symbols
 * @param {Number} length output string length
 * @param {Boolean} right add symbols after instead of before
 * @returns {string}
 */
function strpad(string, pad, length, right)
{
    string = String(string);
    length = length || 2;
    if (string.length >= length || !pad || !pad.length) {
        return string;
    }
    var remaining = length - string.length;
    while (pad.length < remaining) {
        pad += pad;
    }
    pad = pad.substr(0, remaining);
    return right ? string + pad : pad + string;
}

exports.utcStringToMicros = function(value)
{
    var millis = Date.parse(value);
    var micros = 0;
    if (value.indexOf(".") >= 0) {
        value = value.split(".")[1].replace("Z", "");
        value = strpad(value, "0", 6, true);
        micros = parseInt(value) % 1000;
    }
    return millis * 1000 + micros;
};

exports.microsToUtcString = function(value)
{
    var micros = parseInt(value, 10);
    var prefix, suffix;
    prefix = new Date(micros / 1000).toISOString();
    prefix = prefix.replace("Z", "");
    suffix = strpad(micros % 1000, "0", 3) + "Z";
    return prefix + suffix;
};

////////////////////////////////////////////////////////////////////////////////

exports.pick = function(object, keys)
{
    var result = {};
    for (var i = 0, length = keys.length; i < length; ++i) {
        var key = keys[i];
        if (key in object) {
            result[key] = object[key];
        }
    }
    return result;
};

exports.gather = function(object, prefix)
{
    if (typeof(object[prefix]) !== "undefined") {
        return object[prefix];
    }

    var keys = Object.keys(object);
    var result = [];

    var i, n, m, l = prefix.length;
    var kp, ks;

    for (i = 0, n = keys.length; i < n; ++i) {
        kp = keys[i].substr(0, l);
        ks = keys[i].substr(l);
        if (kp === prefix) {
            if (ks[0] === '-') {
                ks = ks.substr(1);
            }
            if (/^[0-9]+$/.test(ks)) {
                m = parseInt(ks);
                if (m < 0 || m > 1000) {
                    throw new Error("Too many header parts for '" + prefix + "'");
                }
                result[parseInt(ks)] = object[keys[i]];
            } else {
                throw new Error("Bad header part '" + keys[i] + "'; suffix '" + ks + "' is not an integer");
            }
        }
    }

    for (i = 0, n = result.length; i < n; ++i) {
        if (typeof(result[i]) !== "string") {
            throw new Error("Missing part " + i + " for header '" + prefix + "'");
        }
    }

    if (result.length === 0) {
        return null;
    } else {
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

exports.NullStream = function()
{
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
    var on_data, on_end, on_error, on_close, events = [];
    var dummy = function() {};

    slave.on("data", on_data = function pause_on_data(data, encoding) {
        events.push(["data", data, encoding]);
    });

    slave.on("end", on_end = function pause_on_end(data, encoding) {
        events.push(["end", data, encoding]);
    });

    slave.on("error", on_error = function pause_on_error(ex) {
        events.push(["error", ex]);
    });

    slave.on("close", on_close = function pause_on_close() {
        events.push(["close"]);
    });

    slave.pause();

    return {
        dispose: function() {
            slave.removeListener("data", on_data);
            slave.removeListener("end", on_end);
            slave.removeListener("error", on_error);
            slave.removeListener("close", on_close);
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

exports.MemoryInputStream = function MemoryInputStream(data)
{
    stream.Stream.call(this);

    var self = this;

    this.paused = false;
    this.readable = true;
    this.writable = false;

    this._emit_data = function _emit_data() {
        if (self.paused || !self.readable) {
            return;
        }
        self._emit_data = function(){};
        if (data) {
            self.emit("data", data);
        }
    };

    this._emit_end = function _emit_end() {
        if (self.paused || !self.readable) {
            return;
        }
        self._emit_end = function(){};
        // Now, perform work.
        self.emit("end", data);
        // Block state.
        self.paused = false;
        self.readable = false;
        self.writable = false;
    };

    this._flow = function() {
        process.nextTick(function() {
            self._emit_data();
            process.nextTick(function() {
                self._emit_end();
            });
        });
    };
};

util.inherits(exports.MemoryInputStream, stream.Stream);

exports.MemoryInputStream.prototype.pause = function MemoryInputStream$pause()
{
    this.paused = true;
};

exports.MemoryInputStream.prototype.resume = function MemoryInputStream$resume()
{
    this.paused = false;
    this._flow();
};

exports.MemoryInputStream.prototype.destroy = function MemoryInputStream$destroy()
{
    this.readable = false;
};

////////////////////////////////////////////////////////////////////////////////

exports.MemoryOutputStream = function MemoryOutputStream()
{
    stream.Stream.call(this);

    this.readable = false;
    this.writable = true;

    this.chunks = [];
};

util.inherits(exports.MemoryOutputStream, stream.Stream);

exports.MemoryOutputStream.prototype.write = function MemoryOutputStream$write(chunk)
{
    if (this.writable && chunk) {
        this.chunks.push(chunk);
    }
    return true;
};

exports.MemoryOutputStream.prototype.end = function MemoryOutputStream$end(chunk)
{
    if (this.writable && chunk) {
        this.chunks.push(chunk);
    }
    this.writable = false;
};

exports.MemoryOutputStream.prototype.destroy = function MemoryOutputStream$destroy()
{
    this.writable = false;
};

////////////////////////////////////////////////////////////////////////////////

exports.getYsonValue = function(x)
{
    if (typeof(x) === "object" && typeof(x.$value) !== "undefined") {
        return x.$value;
    } else {
        return x;
    }
};

exports.getYsonAttributes = function(x)
{
    if (typeof(x) === "object" && typeof(x.$attributes) !== "undefined") {
        return x.$attributes;
    } else {
        return {};
    }
};

exports.getYsonAttribute = function(x, attribute)
{
    return exports.getYsonAttributes(x)[attribute];
};

exports.escapeYPath = function(s)
{
    return s.replace(/([\/@&])/g, '\\$1');
};

exports.escapeHeader = function(x)
{
    return String(x)
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
};

exports.lexicographicalCompare = function(a, b)
{
    for (var i = 0; i < a.length && i < b.length; ++i) {
        if (a[i] === b[i]) {
            continue;
        } else {
            return a[i] - b[i];
        }
    }
    return a.length - b.length;
};

String.prototype.format = function() {
    var i = 0, args = arguments;
    return this.replace(/\{(\d*)\}/g, function(match, key) {
        key = key === "" ? i++ : parseInt(key);
        return typeof(args[key]) !== "undefined" ? args[key] : "";
    });
};
