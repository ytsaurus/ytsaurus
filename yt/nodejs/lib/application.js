var url = require("url");
var crypto = require("crypto");

var qs = require("qs");
var flowless = require("flowless");

var utils = require("./utils");
var ytnode_wrappers = require("./ytnode_wrappers");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { console.error("YT Application:", x); };
} else {
    __DBG = function( ) { };
}

// This mapping defines how MIME types map onto YT format specifications.
var _MIME_FORMAT_MAPPING = {
    "application/json" : "json",
    "application/x-yt-yson-binary" : "<format=binary>yson",
    "application/x-yt-yson-text" : "<format=text>yson",
    "application/x-yt-yson-pretty": "<format=pretty>yson",
    "text/csv" : "csv",
    "text/tab-separated-values" : "dsv",
    "text/x-tskv" : "<line_prefix=tskv>dsv"
};

// This mapping defines which HTTP methods various YT data types require.
var _DATA_TYPE_TO_METHOD_MAPPING = {
    "Null" : "GET",
    "Binary" : "PUT",
    "Structured" : "POST",
    "Tabular" : "PUT"
};

////////////////////////////////////////////////////////////////////////////////

function YtCommand(driver, req, rsp) {
    this.driver = driver;

    this.req = req;
    this.rsp = rsp;

    this.req.parsedUrl = url.parse(this.req.url);
};

YtCommand.prototype.dispatch = function() {
    var self = this;

    flowless.runSeq([
        this._computeHash,
        this._prologue,
        this._extractName,
        this._extractParameters,
        this._getInputFormat,
        this._getOutputFormat,
        this._getDescriptor,
        this._addHeaders,
        this._execute,
        this._epilogue
    ].map(function(func) {
        return func.bind(self);
    }), function andThen(error) {
        if (error) {
            __DBG(self.hash + " <<< Error: " + error.cause.message);

            if (!self.rsp._header) {
                var body = JSON.stringify({ error : error.cause.message });
                self.rsp.setHeader("Content-Type", "application/json");
                self.rsp.setHeader("Content-Length", body.length);
                self.rsp.end(body);
            } else {
                self.rsp.end();
            }
        } else {
            __DBG(self.hash + " <<< Done.");
        }
    });
};

YtCommand.prototype._prologue = function(cb) {
    this.rsp.statusCode = 202;

    __DBG(this.hash + " >>> Dispatching...");
    cb(null);
};

YtCommand.prototype._epilogue = function(cb) {
    cb(null);
};

YtCommand.prototype._computeHash = function(cb) {
    var hasher = crypto.createHash("sha1");
    hasher.update(new Date().toJSON());
    hasher.update(JSON.stringify(this.req.method));
    hasher.update(JSON.stringify(this.req.url));
    hasher.update(JSON.stringify(this.req.headers));
    hasher.update(JSON.stringify(this.req.trailers));
    hasher.update(JSON.stringify(this.req.httpVersion));
    this.hash = hasher.digest("base64");

    cb(null);
};

YtCommand.prototype._extractName = function(cb) {
    this.name = this.req.parsedUrl.pathname.slice(1).toLowerCase();
    if (!/^[a-z_]+$/.test(this.name)) {
        this.rsp.statusCode = 400;
        throw new Error("Malformed command '" + name + "'.");
    }

    __DBG(this.hash + ".name = " + this.name);
    cb(null);
};

YtCommand.prototype._extractParameters = function(cb) {
    this.parameters = utils.numerify(qs.parse(this.req.parsedUrl.query));
    if (!this.parameters) {
        this.rsp.statusCode = 400;
        throw new Error("Unable to parse parameters from the query string.");
    }


    __DBG(this.hash + ".parameters = " + JSON.stringify(this.parameters));
    cb(null);
};

YtCommand.prototype._getInputFormat = function(cb) {
    var result, format, header;

    // Firstly, try to deduce input format from Content-Type header.
    header = this.req.headers["content-type"];
    if (typeof(header) === "string") {
        for (var mime in _MIME_FORMAT_MAPPING) {
            if (utils.is(mime, header)) {
                result = _MIME_FORMAT_MAPPING[mime];
                break;
            }
        }
    }

    // Secondly, try to deduce output format from our custom header.
    header = this.req.headers["x-yt-input-format"];
    if (typeof(header) === "string") {
        result = header;
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        result = "yson";
    }

    this.input_format = result;
    __DBG(this.hash + ".input_format = " + this.input_format);
    cb(null);
};

YtCommand.prototype._getOutputFormat = function(cb) {
    var result, format, header;

    // Firstly, try to deduce output format from Accept header.
    header = this.req.headers["accept"];
    if (typeof(header) === "string") {
        for (var mime in _MIME_FORMAT_MAPPING) {
            if (mime === utils.accepts(mime, header)) {
                result = _MIME_FORMAT_MAPPING[mime];
                this.rsp.setHeader("Content-Type", mime);
                break;
            }
        }
    }

    // Secondly, try to deduce output format from our custom header.
    header = this.req.headers["x-yt-output-format"];
    if (typeof(header) === "string") {
        result = header;
        this.rsp.setHeader("Content-Type", "application/octet-stream");
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        result = "<format=pretty;enable_raw=false>yson";
        this.rsp.setHeader("Content-Type", "text/plain");
    }

    this.output_format = result;
    __DBG(this.hash + ".output_format = " + this.output_format);
    cb(null);
};

YtCommand.prototype._getDescriptor = function(cb) {
    this.descriptor = this.driver.find_command_descriptor(this.name);
    if (!this.descriptor) {
        this.rsp.statusCode = 404;
        throw new Error("There is no such command '" + this.name + "' registered.");
    }

    __DBG(this.hash + ".descriptor = " + JSON.stringify(this.descriptor));

    var input_type_as_string = ytnode_wrappers.EDataType[this.descriptor.input_type];
    var output_type_as_string = ytnode_wrappers.EDataType[this.descriptor.output_type];

    __DBG(this.hash + ".input_type_as_string = " + input_type_as_string);
    __DBG(this.hash + ".output_type_as_string = " + output_type_as_string);

    var expected_http_method = _DATA_TYPE_TO_METHOD_MAPPING[input_type_as_string];
    var actual_http_method = this.req.method;

    __DBG(this.hash + ".expected_http_method = " + expected_http_method);
    __DBG(this.hash + ".actual_http_method = " + actual_http_method);

    if (expected_http_method != actual_http_method) {
        this.rsp.statusCode = 405;
        this.rsp.setHeader("Allow", expected_http_method);
        throw new Error("Command '" + this.name + "' expects " + input_type_as_string.toLowerCase() + " input and hence have to be requested with the " + expected_http_method + " method.");
    }

    cb(null);
};

YtCommand.prototype._addHeaders = function(cb) {
    this.rsp.setHeader("Connection", "close");
    this.rsp.setHeader("Transfer-Encoding", "chunked");
    this.rsp.setHeader("Trailer", "X-YT-Response-Code, X-YT-Response-Message");
    this.rsp.setHeader("X-YT-Request-Hash", this.hash);
    cb(null);
};

YtCommand.prototype._execute = function(cb) {
    var self = this;
    this.driver.execute(this.name,
        this.req, this.input_format,
        this.rsp, this.output_format,
        this.parameters, function(code, message)
        {
            if (code === 0) {
                self.rsp.statusCode = 200;
            } else if (code < 0) {
                self.rsp.statusCode = 500;
            } else if (code > 0) {
                self.rsp.statusCode = 400;
            }

            if (code != 0) {
                self.rsp.addTrailers({
                    "X-YT-Response-Code" : JSON.stringify(code),
                    "X-YT-Response-Message" : JSON.stringify(message)
                });
            }

            cb(null);
        });
};

////////////////////////////////////////////////////////////////////////////////

function YtApplication(configuration) {
    var driver = new ytnode_wrappers.YtDriver(configuration);
    return function(req, rsp) {
        return (new YtCommand(driver, req, rsp)).dispatch();
    };
};

////////////////////////////////////////////////////////////////////////////////

exports.YtApplication = YtApplication;
