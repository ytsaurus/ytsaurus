/*jshint forin: false*/

////////////////////////////////////////////////////////////////////////////////
// These functions are mainly from Express framework.
// http://expressjs.com/
////////////////////////////////////////////////////////////////////////////////

var util = require("util");
var stream = require("stream");

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

exports.acceptsType = function(mime, header) {
    "use strict";
    if (!header) {
        return false;
    }

    var parts = mime.split("/");
    var accepted = exports.parseAcceptType(header);
    for (var i = 0, imax = accepted.length; i < imax; ++i) {
        if ((accepted[i].type    === parts[0] || accepted[i].type    === "*") &&
            (accepted[i].subtype === parts[1] || accepted[i].subtype === "*"))
        {
            return true;
        }
    }

    return false;
};

exports.acceptsEncoding = function(encoding, header) {
    "use strict";
    if (!header) {
        return false;
    }

    var accepted = exports.parseAcceptEncoding(header);
    for (var i = 0, imax = accepted.length; i < imax; ++i) {
        if (accepted[i].value === encoding || accepted[i].value === "*")
        {
            return true;
        }
    }

    return false;
};

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

exports.parseQuality = function(header) {
    "use strict";

    var parts = header.split(/ *; */);
    var value = parts[0];

    var quality = parts[1] ? parseFloat(parts[1].split(/ *= */)[1]) : 1.0;

    return { value: value, quality: quality };
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
 * A simple control flow function which sequentially calls all functions.
 */
exports.callSeq = function(context, functions, callback) {
    "use strict";
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
            callback.call(context);
        }
    }(context, functions, callback));
};

/**
 * A simple control flow conditional branch.
 */
exports.callIf = function(context, condition, if_true, if_false) {
    "use strict";
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

    process.nextTick(function() { self.emit("end"); self.readable = false; });
};

util.inherits(exports.NullStream, stream.Stream);

exports.NullStream.prototype.pause = function(){};
exports.NullStream.prototype.resume = function(){};
exports.NullStream.prototype.destroy = function(){};
