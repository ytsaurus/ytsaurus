var __DBG = require("./debug").that("A", "HTTP");

var url = require("url");
var http = require("http");
var buffertools = require("buffertools");
var Q = require("q");

function YtHttp(host, port, path, verb, body)
{
    this.host = host;
    this.port = port != null ? port : 80;
    this.path = path != null ? path : "/";
    this.verb = verb != null ? verb : "GET";
    this.body = body != null ? body : null;
    this.headers = { "User-Agent": "YT" };
    this.nodelay = true;
    this.timeout = 15000;
}

YtHttp.prototype.withHost = function(host)
{
    this.host = host;
    return this;
};

YtHttp.prototype.withPort = function(port)
{
    this.port = port;
    return this;
};

YtHttp.prototype.withPath = function(path)
{
    this.path = path;
    return this;
};

YtHttp.prototype.withVerb = function(verb)
{
    this.verb = verb;
    return this;
};

YtHttp.prototype.setNoDelay = function(nodelay)
{
    this.nodelay = nodelay;
    return this;
};

YtHttp.prototype.setTimeout = function(timeout)
{
    this.timeout = timeout;
    return this;
};

YtHttp.prototype.withBody = function(body, type)
{
    if (typeof(this.body) === "object") {
        this.body = JSON.stringify(body);
    } else {
        this.body = body;
    }

    this.headers["Content-Type"] = type;
    this.headers["Content-Length"] = \
        typeof(this.body) === "string" \
        ? Buffer.byteLength(this.body)
        : this.body.length;

    return this;
};

YtHttp.prototype.withHeader = function(header, value)
{
    this.headers[header] = value;
    return this;
};

// TODO(sandello): Use YtError here to provide nested errors.
YtHttp.prototype.fire = function()
{
    __DBG("Firing a request to '" + this.host + "/" + this.path + "'");

    var deferred = Q.defer();
    var request = http.request({
        method: this.verb,
        headers: this.headers,
        host: this.host,
        path: url.format({ pathname: this.path })
    });

    request.setNoDelay(this.nodelay);
    request.setTimeout(this.timeout, function() {
        deferred.reject(new Error("Request timed out"));
    });
    request.once("error", function(error) {
        deferred.reject(error);
    });
    request.once("response", function(response) {
        var chunks = [];
        response.on("data", function(chunk) { chunks.push(chunk); });
        response.on("end", function() {
            deferred.resolve(buffertools.concat.apply(buffertools, chunks));
        });
    });
    request.end(this.body);

    return deferred.promise;
};

exports.that = YtHttp;
