var url = require("url");
var http = require("http");
var https = require("https");
var buffertools = require("buffertools");
var Q = require("bluebird");

var YtError = require("./error").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("A", "HTTP");

////////////////////////////////////////////////////////////////////////////////

function YtHttpRequest(host, port, path, verb, body)
{
    "use strict";
    if (!(this instanceof YtHttpRequest)) {
        return new YtHttpRequest(host, port, path, verb, body);
    }

    this.host = host;
    this.port = typeof(port) !== "undefined" ? port : 80;
    this.path = typeof(path) !== "undefined" ? path : "/";
    this.verb = typeof(verb) !== "undefined" ? verb : "GET";
    this.secure = false;
    this.json = false;
    this.body = null;
    this.headers = {};
    this.nodelay = true;
    this.timeout = 15000;

    this.failOn4xx = false;
    this.failOn5xx = true;

    this.withBody(body).withHeader("User-Agent", "YT");
}

YtHttpRequest.prototype.withHost = function(host)
{
    "use strict";
    this.host = host;
    return this;
};

YtHttpRequest.prototype.withPort = function(port)
{
    "use strict";
    this.port = port;
    return this;
};

YtHttpRequest.prototype.withPath = function(path)
{
    "use strict";
    this.path = path;
    return this;
};

YtHttpRequest.prototype.withVerb = function(verb)
{
    "use strict";
    this.verb = verb;
    return this;
};

YtHttpRequest.prototype.setNoDelay = function(nodelay)
{
    "use strict";
    this.nodelay = nodelay;
    return this;
};

YtHttpRequest.prototype.setTimeout = function(timeout)
{
    "use strict";
    this.timeout = timeout;
    return this;
};

YtHttpRequest.prototype.asHttps = function(secure)
{
    "use strict";
    this.secure = !!secure;
    return this;
};

YtHttpRequest.prototype.asJson = function(json)
{
    "use strict";
    this.json = !!json;
    return this;
};

YtHttpRequest.prototype.shouldFailOn4xx = function(fail)
{
    "use strict";
    this.failOn4xx = !!fail;
    return this;
};

YtHttpRequest.prototype.shouldFailOn5xx = function(fail)
{
    "use strict";
    this.failOn5xx = !!fail;
    return this;
};

YtHttpRequest.prototype.withBody = function(body, type)
{
    "use strict";
    if (typeof(body) === "object") {
        this.body = JSON.stringify(body);
        type = "application/json";
    } else {
        this.body = body;
    }

    if (typeof(type) === "string") {
        this.headers["Content-Type"] = type;
    }

    if (typeof(this.body) !== "undefined") {
        this.headers["Content-Length"] =
            typeof(this.body) === "string" ?
            Buffer.byteLength(this.body) :
            this.body.length;
    } else {
        this.headers["Content-Length"] = 0;
    }

    return this;
};

YtHttpRequest.prototype.withHeader = function(header, value)
{
    "use strict";
    this.headers[header] = value;
    return this;
};

YtHttpRequest.prototype.fire = function()
{
    "use strict";
    __DBG("Firing: " + this.toString());

    var self = this;

    var deferred = Q.defer();
    var proto = self.secure ? https : http;
    var req = proto.request({
        method: self.verb,
        headers: self.headers,
        host: self.host,
        port: self.port,
        path: self.path,
    });

    req.setNoDelay(self.nodelay);
    req.setTimeout(self.timeout, function() {
        deferred.reject(new YtError(
            self.toString() + " has timed out"));
    });
    req.once("error", function(err) {
        deferred.reject(new YtError(
            self.toString() + " has failed", err));
    });
    req.once("response", function(rsp) {
        var code = rsp.statusCode;
        if (
            (self.failOn4xx && code >= 400 && code < 500) ||
            (self.failOn5xx && code >= 500 && code < 600))
        {
            deferred.reject(new YtError(
                self.toString() + " has responded with " + rsp.statusCode));
            return;
        }

        var chunks = [];
        var result;
        rsp.on("data", function(chunk) {
            chunks.push(chunk);
        });
        rsp.on("end", function() {
            result = buffertools.concat.apply(undefined, chunks);
            if (!self.json) {
                deferred.resolve(result);
            } else {
                try {
                    deferred.resolve(JSON.parse(result));
                } catch (err) {
                    deferred.reject(new YtError(
                        self.toString() + " has responded with invalid JSON",
                        err));
                }
            }
        });
    });
    req.end(self.body);

    return deferred.promise;
};

YtHttpRequest.prototype.toString = function()
{
    "use strict";
    return "Request to '" + this.host + ":" + this.port + this.path + "'";
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtHttpRequest;
