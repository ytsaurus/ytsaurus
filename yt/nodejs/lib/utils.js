/*jshint forin: false*/

////////////////////////////////////////////////////////////////////////////////
// These functions are mainly from Express framework.
// http://expressjs.com/
////////////////////////////////////////////////////////////////////////////////

var util = require("util");
var stream = require("stream");

// Checks whether MIME pattern |mime| matches actual MIME type |actual|.
exports.matches = function(mime, actual) {
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
exports.bestAcceptedType = function(mimes, header) {
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
exports.bestAcceptedEncoding = function(encodings, header) {
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
exports.parseAcceptType = function(header) {
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
exports.parseAcceptEncoding = function(header) {
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
exports.parseQuality = function(header) {
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
exports.numerify = function(obj) {
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
exports.merge = function (lhs, rhs) {
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
        } catch(err) {
            lhs[p] = rhs[p];
        }
    }
    return lhs;
};

////////////////////////////////////////////////////////////////////////////////

exports.NullStream = function() {
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

exports.Pause = function(slave) {
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
