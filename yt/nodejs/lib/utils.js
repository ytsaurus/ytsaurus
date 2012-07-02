/*jshint strict: false, forin: false*/

// These functions are mainly from Express framework.
// http://expressjs.com/
////////////////////////////////////////////////////////////////////////////////

var util = require("util");
var stream = require("stream");

/**
 * Check if `type` matches given `str`.
 */
exports.is = function(type, str) {
    'use strict';
    if (!str) {
        return false;
    }

    str = str.split(";")[0];

    if (type.indexOf("*") !== -1) {
        type = type.split("/");
        str = str.split("/");

        return (type[0] === "*" && type[1] === str[1]) ||
               (type[1] === "*" && type[0] === str[0]) ||
               (type[0] === "*" && type[1] === "*");
    }

    return type === str;
};

/**
 * Check if `type(s)` are acceptable based on the given `str`.
 */
exports.accepts = function(mime, str) {
    'use strict';
    if (!str) {
        return false;
    }

    var accepted = exports.parseAccept(str);
    for (var i = 0, imax = accepted.length; i < imax; ++i) {
        if (exports.testAccept(mime, accepted[i])) {
            return true;
        }
    }

    return false;
};

exports.acceptsEncoding = function(encoding, str) {
    'use strict';
    if (!str) {
        return false;
    }

    var accepted = exports.parseAcceptEncoding(str);
    for (var i = 0, imax = accepted.length; i < imax; ++i) {
        if (accepted[i].value === "*" || accepted[i].value === encoding) {
            return true;
        }
    }

    return false;
};

/**
 * Parse Accept `str`, returning an array objects containing
 * `.type` and `.subtype` along with the values provided by
 * `parseQuality()`.
 */

exports.parseAccept = function(str) {
    'use strict';
    return str
        .split(/ *, */)
        .map(exports.parseQuality)
        .filter(function(obj) {
            return obj.quality;
        })
        .sort(function(a, b) {
            return b.quality - a.quality;
        })
        .map(function(obj) {
            var parts = obj.value.split("/");
            obj.type = parts[0];
            obj.subtype = parts[1];
            return obj;
        });
};

exports.parseAcceptEncoding = function(str) {
    'use strict';
    return str
        .split(/ *, */)
        .map(exports.parseQuality)
        .filter(function(obj) {
            return obj.quality;
        })
        .sort(function(a, b) {
            return b.quality - a.quality;
        });
};

/**
 * Parse quality `str`, returning an object with `.value` and `.quality`.
 */

exports.parseQuality = function(str) {
    'use strict';
    var parts = str.split(/ *; */);
    var value = parts[0];

    var quality = parts[1] ? parseFloat(parts[1].split(/ *= */)[1]) : 1.0;

    return { value: value, quality: quality };
};

/**
 * Check if `type` array is acceptable for `other`.
 */

exports.testAccept = function(type, other) {
    'use strict';
    var parts = type.split("/");
    return (parts[0] === other.type || "*" === other.type) &&
           (parts[1] === other.subtype || "*" === other.subtype);
};

/**
 * Recursively traverses non-cyclic structure and replaces everything
 * that looks like a number with a number.
 */
exports.numerify = function(obj) {
    'use strict';
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
 * A simple control flow function which sequentially calls all functions.
 */
exports.callSeq = function(context, functions, callback) {
    'use strict';
    return (function inner(context, functions, callback) {
        var nextFunction = functions.shift();
        if (typeof(nextFunction) !== "undefined") {
            try {
                nextFunction.call(context, function(ex) {
                    if (typeof(ex) === "undefined") {
                        inner(context, functions, callback);
                    } else {
                        callback.call(context, ex);
                    }
                });
            } catch(ex) {
                callback.call(context, ex);
            }
        } else {
            callback.call(context, null);
        }
    }(context, functions, callback));
};

exports.callIf = function(context, condition, if_true, if_false) {
    'use strict';
    return function(cb) {
        if (condition.call(context)) {
            if (if_true) {
                if_true.call(context, cb);
            }
        } else {
            if (if_false) {
                if_false.call(context, cb);
            }
        }
    };
};

exports.merge = function (lhs, rhs) {
    'use strict';
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
    'use strict';
    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    var self = this;

    process.nextTick(function() { self.emit("end"); self.readable = false; });
};

util.inherits(exports.NullStream, stream.Stream);

exports.NullStream.prototype.pause = function() { };
exports.NullStream.prototype.resume = function() { };
exports.NullStream.prototype.destroy = function() { };
