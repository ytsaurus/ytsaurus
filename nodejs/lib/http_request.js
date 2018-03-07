var buffertools = require("buffertools");
var dns = require("dns");
var url = require("url");
var http = require("http");
var https = require("https");
var buffertools = require("buffertools");
var Q = require("bluebird");

var YtError = require("./error").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("A", "HTTP");

var _resolveIPv4 = Q.promisify(dns.resolve4);
var _resolveIPv6 = Q.promisify(dns.resolve6);

////////////////////////////////////////////////////////////////////////////////

function _replaceUrlParameter(url, param, value)
{
    // From https://gist.github.com/stinoga/8101816
    var re = new RegExp("[\\?&]" + param + "=([^&#]*)");
    var match = re.exec(url);
    var delimiter;
    var newUrl = url;

    if (match !== null) {
        delimiter = match[0].charAt(0);
        newUrl = newUrl.replace(re, delimiter + param + "=" + value);
    }

    return newUrl;
}

function YtHttpRequest(host, port, path, verb, body)
{
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
    this.headers = {"Host": host};
    this.nodelay = true;
    this.noresolve = false;
    this.timeout = 15000;

    this.failOn4xx = false;
    this.failOn5xx = true;

    this.withBody(body).withHeader("User-Agent", "YT");
}

YtHttpRequest.prototype.withHost = function(host)
{
    this.host = host;
    this.headers["Host"] = host;
    return this;
};

YtHttpRequest.prototype.withPort = function(port)
{
    this.port = port;
    return this;
};

YtHttpRequest.prototype.withPath = function(path)
{
    this.path = path;
    return this;
};

YtHttpRequest.prototype.withVerb = function(verb)
{
    this.verb = verb;
    return this;
};

YtHttpRequest.prototype.setNoDelay = function(nodelay)
{
    this.nodelay = nodelay;
    return this;
};

YtHttpRequest.prototype.setNoResolve = function(noresolve)
{
    this.noresolve = noresolve;
    return this;
};

YtHttpRequest.prototype.setTimeout = function(timeout)
{
    this.timeout = timeout;
    return this;
};

YtHttpRequest.prototype.asHttps = function(secure)
{
    this.secure = !!secure;
    return this;
};

YtHttpRequest.prototype.asJson = function(json)
{
    this.json = !!json;
    return this;
};

YtHttpRequest.prototype.shouldFailOn4xx = function(fail)
{
    this.failOn4xx = !!fail;
    return this;
};

YtHttpRequest.prototype.shouldFailOn5xx = function(fail)
{
    this.failOn5xx = !!fail;
    return this;
};

YtHttpRequest.prototype.withBody = function(body, type)
{
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
    this.headers[header] = value;
    return this;
};

YtHttpRequest.prototype.fire = function()
{
    var self = this;

    function impl(addr, resolve, reject) {
        __DBG("Firing: " + self.toString() + " via " + addr.toString());

        var proto = self.secure ? https : http;
        var req = proto.request({
            method: self.verb,
            headers: self.headers,
            host: addr,
            port: self.port,
            path: self.path,
            agent: null,
        });

        req.setNoDelay(self.nodelay);
        req.setTimeout(self.timeout, function() {
            reject(new YtError(self.toDebugString() + " has timed out"));
        });
        req.once("error", function(err) {
            reject(new YtError(self.toDebugString() + " has failed", err));
        });
        req.once("response", function(rsp) {
            var code = rsp.statusCode;
            if (
                (self.failOn4xx && code >= 400 && code < 500) ||
                (self.failOn5xx && code >= 500 && code < 600))
            {
                reject(new YtError(self.toDebugString() + " has responded with " + rsp.statusCode));
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
                    resolve(result);
                } else {
                    try {
                        resolve(JSON.parse(result));
                    } catch (err) {
                        reject(new YtError(self.toDebugString() + " has responded with invalid JSON", err));
                    }
                }
            });
        });
        req.end(self.body);
        return req;
    }

    var req = new Q.defer();
    var promise = new Q(function(resolve, reject) {
        if (self.noresolve) {
            req.resolve(impl(self.host, resolve, reject));
        } else {
            _resolveIPv6(self.host).then(
                function(addrs) {
                    if (addrs.length === 0) {
                        reject(new YtError(self.toDebugString() + " has resolved " + self.host + " to empty host set"));
                        req.resolve({abort: function(){}});
                    } else {
                        req.resolve(impl(addrs[0], resolve, reject));
                    }
                },
                function(err6) {
                    return _resolveIPv4(self.host).then(
                        function(addrs) {
                            if (addrs.length === 0) {
                                reject(new YtError(self.toDebugString() + " has resolved " + self.host + " to empty host set"));
                                req.resolve({abort: function(){}});
                            } else {
                                req.resolve(impl(addrs[0], resolve, reject));
                            }
                        },
                        function(err4) {
                            var error = new YtError(self.toDebugString() + " has failed to resolve " + self.host);
                            if (err6) {
                                error.withNested(err6);
                            }
                            if (err4) {
                                error.withNested(err4);
                            }
                            req.resolve({abort: function(){}});
                            reject(error);
                        });
                });
        }
    });

    promise = promise.timeout(
        self.timeout * 1.05,
        new YtError(self.toDebugString() + " has timed out (hardly)"));

    promise.catch(function() {
        req.promise.then(function(r) { r.abort(); });
    });

    return promise;
};

YtHttpRequest.prototype.toDebugString = function()
{
    return "Request to '" + this.host + ":" + this.port + _replaceUrlParameter(this.path, 'oauth_token', 'XXXXX') + "'";
};

YtHttpRequest.prototype.toString = function()
{
    return "Request to '" + this.host + ":" + this.port + this.path + "'";
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtHttpRequest;
