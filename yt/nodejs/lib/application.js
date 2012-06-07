var url = require("url");
var crypto = require("crypto");

var qs = require("qs");

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
var _MAPPING_MIME_TYPE_TO_FORMAT = {
    "application/json" : "json",
    "application/x-yt-yson-binary" : "<format=binary>yson",
    "application/x-yt-yson-text" : "<format=text>yson",
    "application/x-yt-yson-pretty": "<format=pretty>yson",
    "text/csv" : "csv",
    "text/tab-separated-values" : "dsv",
    "text/x-tskv" : "<line_prefix=tskv>dsv"
};

// This mapping defines which HTTP methods various YT data types require.
var _MAPPING_DATA_TYPE_TO_METHOD = {
    "Null" : "GET",
    "Binary" : "PUT",
    "Structured" : "POST",
    "Tabular" : "PUT"
};

////////////////////////////////////////////////////////////////////////////////

function YtCommand(logger, driver, req, rsp) {
    this.logger = logger;
    this.driver = driver;

    this.req = req;
    this.rsp = rsp;

    this.req.parsedUrl = url.parse(this.req.url);
};

YtCommand.prototype.dispatch = function() {
    var self = this;

    utils.callSeq(this, [
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
    ], function andThen(error) {
        var thereWasError = error || self.rsp.ytCode != 0;
        if (thereWasError) {
            var message = error ? error.cause.message : self.rsp.ytMessage;
            self.logger.error(message, { hash : self.hash });

            if (!self.rsp._header) {
                var body = JSON.stringify({ error : message });
                self.rsp.removeHeader("Transfer-Encoding");
                self.rsp.setHeader("Content-Type", "application/json");
                self.rsp.setHeader("Content-Length", body.length);
                self.rsp.end(body);
            } else {
                self.rsp.end();
            }
        }

        self.logger.info("Done", { hash : self.hash });
    });
};

YtCommand.prototype._prologue = function(cb) {
    this.rsp.statusCode = 202;

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
    this.hash = hasher.digest("base64");
    cb(null);
};

YtCommand.prototype._extractName = function(cb) {
    this.name = this.req.parsedUrl.pathname.slice(1).toLowerCase();
    if (!/^[a-z_]+$/.test(this.name)) {
        this.rsp.statusCode = 400;
        throw new Error("Malformed command name '" + name + "'.");
    }

    this.logger.debug("", { hash : this.hash, name : this.name });
    cb(null);
};

YtCommand.prototype._extractParameters = function(cb) {
    this.parameters = utils.numerify(qs.parse(this.req.parsedUrl.query));
    if (!this.parameters) {
        this.rsp.statusCode = 400;
        throw new Error("Unable to parse parameters from the query string.");
    }

    this.logger.debug("", { hash : this.hash, parameters : JSON.stringify(this.parameters) });
    cb(null);
};

YtCommand.prototype._getInputFormat = function(cb) {
    var result, format, header;

    // Firstly, try to deduce input format from Content-Type header.
    header = this.req.headers["content-type"];
    if (typeof(header) === "string") {
        for (var mime in _MAPPING_MIME_TYPE_TO_FORMAT) {
            if (utils.is(mime, header)) {
                result = _MAPPING_MIME_TYPE_TO_FORMAT[mime];
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
    this.logger.debug("", { hash : this.hash, input_format : this.input_format });
    cb(null);
};

YtCommand.prototype._getOutputFormat = function(cb) {
    var result, format, header;

    // Firstly, try to deduce output format from Accept header.
    header = this.req.headers["accept"];
    if (typeof(header) === "string") {
        for (var mime in _MAPPING_MIME_TYPE_TO_FORMAT) {
            if (utils.accepts(mime, header)) {
                result = _MAPPING_MIME_TYPE_TO_FORMAT[mime];
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
    this.logger.debug("", { hash : this.hash, output_format : this.output_format });
    cb(null);
};

YtCommand.prototype._getDescriptor = function(cb) {
    this.descriptor = this.driver.find_command_descriptor(this.name);
    if (!this.descriptor) {
        this.rsp.statusCode = 404;
        throw new Error("Command '" + this.name + "' is not registered.");
    }

    var input_type_as_string = ytnode_wrappers.EDataType[this.descriptor.input_type];
    var output_type_as_string = ytnode_wrappers.EDataType[this.descriptor.output_type];

    if (output_type_as_string == "Binary") {
        this.rsp.setHeader("Content-Type", "application/octet-stream");
    }

    var expected_http_method = _MAPPING_DATA_TYPE_TO_METHOD[input_type_as_string];
    var actual_http_method = this.req.method;

    this.logger.debug("Successfully found command descriptor", {
        hash : this.hash,
        descriptor : JSON.stringify(this.descriptor),
        input_type_as_string : input_type_as_string,
        output_type_as_string : output_type_as_string,
        expected_http_method : expected_http_method,
        actual_http_method : actual_http_method
    });

    if (expected_http_method != actual_http_method) {
        this.rsp.statusCode = 405;
        this.rsp.setHeader("Allow", expected_http_method);
        throw new Error(
            "Command '" + this.name + "' expects " + input_type_as_string.toLowerCase() +
            " input and hence have to be requested with the " + expected_http_method + " HTTP method.");
    }

    cb(null);
};

YtCommand.prototype._addHeaders = function(cb) {
    this.rsp.setHeader("Connection", "close");
    this.rsp.setHeader("Transfer-Encoding", "chunked");
    this.rsp.setHeader("Access-Control-Allow-Origin", "*");
    this.rsp.setHeader("Trailer", "X-YT-Response-Code, X-YT-Response-Message");
    this.rsp.setHeader("X-YT-Request-Hash", this.hash);

    cb(null);
};

YtCommand.prototype._execute = function(cb) {
    var self = this;
    this.driver.execute(this.name,
        this.req, this.input_format,
        this.rsp, this.output_format,
        this.parameters, function(error, code, message)
        {
            if (error) {
                return cb(new Error(error));
            }

            self.rsp.ytCode = code;
            self.rsp.ytMessage = message;

            self.logger.info("Command '" + self.name + "' successfully executed", { hash : self.hash, code : code, message : JSON.stringify(message) });

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

function YtApplication(logger, configuration) {
    var driver = new ytnode_wrappers.YtDriver(configuration);
    return function(req, rsp) {
        return (new YtCommand(logger, driver, req, rsp)).dispatch();
    };
};

////////////////////////////////////////////////////////////////////////////////

exports.YtApplication = YtApplication;
