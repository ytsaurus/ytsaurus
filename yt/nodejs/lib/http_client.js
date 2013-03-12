var __DBG = require("./debug").that("A", "HTTP");

var url = require("url");
var http = require("http");
var buffertools = require("buffertools");
var Q = require("q");

function YtHttpClient(host, port, path, verb, body)
{
    if (!(this instanceof YtHttpClient)) {
        return new YtHttpClient(host, port, path, verb, body);
    }

    this.host = host;
    this.port = port != null ? port : 80;
    this.path = path != null ? path : "/";
    this.verb = verb != null ? verb : "GET";
    this.body = null;
    this.headers = {};
    this.nodelay = true;
    this.timeout = 15000;

    this.withBody(body).withHeader("User-Agent", "YT");
}

YtHttpClient.prototype.withHost = function(host)
{
    this.host = host;
    return this;
};

YtHttpClient.prototype.withPort = function(port)
{
    this.port = port;
    return this;
};

YtHttpClient.prototype.withPath = function(path)
{
    this.path = path;
    return this;
};

YtHttpClient.prototype.withVerb = function(verb)
{
    this.verb = verb;
    return this;
};

YtHttpClient.prototype.setNoDelay = function(nodelay)
{
    this.nodelay = nodelay;
    return this;
};

YtHttpClient.prototype.setTimeout = function(timeout)
{
    this.timeout = timeout;
    return this;
};

YtHttpClient.prototype.withBody = function(body, type)
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

    this.headers["Content-Length"] = \
        typeof(this.body) === "string" \
        ? Buffer.byteLength(this.body)
        : this.body.length;

    return this;
};

YtHttpClient.prototype.withHeader = function(header, value)
{
    this.headers[header] = value;
    return this;
};

// TODO(sandello): Use YtError here to provide nested errors.
YtHttpClient.prototype.fire = function()
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
        deferred.reject(new Error(
            "Request to '" + this.host + "/" + this.path + "' timed out"));
    });
    request.once("error", function(error) {
        deferred.reject(error);
    });
    request.once("response", function(response) {
        var chunks = [];
        response.on("data", function(chunk) {
            chunks.push(chunk);
        });
        response.on("end", function() {
            deferred.resolve(buffertools.concat.apply(buffertools, chunks));
        });
    });
    request.end(this.body);

    return deferred.promise;
};

exports.that = YtHttpClient;
