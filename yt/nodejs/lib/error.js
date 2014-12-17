/* jshint proto: false */
var util = require("util");

var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

function YtError(first, second) {
    "use strict";

    this.code = 0;
    this.message = "";
    this.attributes = {};
    this.inner_errors = [];

    if (typeof(first) !== "undefined") {
        if (first instanceof YtError) {
            this.code = first.code;
            this.message = first.message;
            this.attributes = first.attributes;
            this.inner_errors = first.inner_errors;
        } else if (first instanceof Error) {
            this.code = YtError.V8_ERROR_CODE;
            this.message = first.message;
            this.attributes.stack = JSON.stringify(first.stack);
        } else {
            this.code = YtError.JS_ERROR_CODE;
            this.message = first.toString();
        }

        if (second) {
            this.inner_errors.push(YtError.ensureWrapped(second));
        }
    }
}

YtError.V8_ERROR_CODE = -1;
YtError.JS_ERROR_CODE = -2;

util.inherits(YtError, Error);
binding.BasicYtError.prototype.__proto__ = YtError.prototype; // As in buffer.js

// Static method.

YtError.ensureWrapped = function(err, message)
{
    "use strict";
    if (err instanceof YtError) {
        return err;
    } else {
        if (message) {
            return new YtError(message, err);
        } else {
            return new YtError(err);
        }
    }
};

// Is OK?

function checkForErrorCode(error, code)
{
    if (error.code === code) {
        return true;
    }
    for (var i = 0, n = error.inner_errors.length; i < n; ++i) {
        if (checkForErrorCode(error.inner_errors[i], code)) {
            return true;
        }
    }
    return false;
}

YtError.prototype.isOK = function() {
    "use strict";
    return this.code === 0;
};

YtError.prototype.isUnavailable = function() {
    "use strict";
    return checkForErrorCode(this, binding.UnavailableYtErrorCode);
};

YtError.prototype.isUserBanned = function() {
    "use strict";
    return checkForErrorCode(this, binding.UserBannedYtErrorCode);
};

YtError.prototype.isRequestRateLimitExceeded = function() {
    "use strict";
    return checkForErrorCode(this, binding.RequestRateLimitExceededYtErrorCode);
};

YtError.prototype.isAllTargetNodesFailed = function() {
    "use strict";
    return checkForErrorCode(this, binding.AllTargetNodesFailedYtErrorCode);
};

YtError.prototype.checkFor = function(code) {
    "use strict";
    return checkForErrorCode(this, code);
};

// Setters.

YtError.prototype.withCode = function(code) {
    "use strict";
    this.code = code;
    return this;
};

YtError.prototype.withMessage = function(message) {
    "use strict";
    this.message = message;
    return this;
};

YtError.prototype.withAttribute = function(key, value) {
    "use strict";
    this.attributes[key] = value;
    return this;
};

// Getters.

YtError.prototype.getCode = function() {
    "use strict";
    return this.code;
};

YtError.prototype.getMessage = function() {
    "use strict";
    return this.message;
};

YtError.prototype.getAttribute = function(key) {
    "use strict";
    return this.attributes[key];
};

// Serialization.

YtError.prototype.toJson = function() {
    "use strict";
    var p;
    var serialized_attributes = [];
    var serialized_inner_errors = [];
    for (p in this.attributes) {
        if (this.attributes.hasOwnProperty(p)) {
            serialized_attributes.push(JSON.stringify(p) + ':' + this.attributes[p]);
        }
    }
    for (p in this.inner_errors) {
        if (this.inner_errors.hasOwnProperty(p)) {
            serialized_inner_errors.push(this.inner_errors[p].toJson());
        }
    }
    return '{' +
        '"code":' + JSON.stringify(this.code) + ',' +
        '"message":' + JSON.stringify(this.message) + ',' +
        '"attributes":{' + serialized_attributes.join(',') + '},' +
        '"inner_errors":[' + serialized_inner_errors.join(',') + ']' +
        '}';
};

YtError.prototype.toString = function() {
    "use strict";
    return "YtError: " + this.message;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtError;
