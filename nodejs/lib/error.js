/* jshint proto: false */
var util = require("util");

var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

function YtError(first, second) {
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
    return this.code === 0;
};

YtError.prototype.isUnavailable = function() {
    return checkForErrorCode(this, binding.NRpc_UnavailableYtErrorCode);
};

YtError.prototype.isUserBanned = function() {
    return checkForErrorCode(this, binding.NSecurityClient_UserBannedYtErrorCode);
};

YtError.prototype.isRequestQueueSizeLimitExceeded = function() {
    return false ||
        checkForErrorCode(this, binding.NSecurityClient_RequestQueueSizeLimitExceededYtErrorCode) ||
        checkForErrorCode(this, binding.NRpc_RequestQueueSizeLimitExceededYtErrorCode);
};

YtError.prototype.isAllTargetNodesFailed = function() {
    return checkForErrorCode(this, binding.NChunkClient_AllTargetNodesFailedYtErrorCode);
};

YtError.prototype.checkFor = function(code) {
    return checkForErrorCode(this, code);
};

YtError.prototype.withNested = function(err) {
    this.inner_errors.push(YtError.ensureWrapped(err));
    return this;
};

// Setters.

YtError.prototype.withCode = function(code) {
    this.code = code;
    return this;
};

YtError.prototype.withMessage = function(message) {
    this.message = message;
    return this;
};

YtError.prototype.withRawAttribute = function(key, value) {
    this.attributes[key] = value;
    return this;
};

YtError.prototype.withAttribute = function(key, value) {
    this.attributes[key] = JSON.stringify(value);
    return this;
};

// Getters.

YtError.prototype.getCode = function() {
    return this.code;
};

YtError.prototype.getMessage = function() {
    return this.message;
};

YtError.prototype.getAttribute = function(key) {
    return this.attributes[key];
};

// Serialization.

YtError.prototype.toJson = function() {
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
    return "YtError: " + this.message;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtError;
