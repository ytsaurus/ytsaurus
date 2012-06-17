var url = require("url");
var crypto = require("crypto");

var qs = require("qs");
var uuid = require("node-uuid");

var utils = require("./utils");
var ytnode_wrappers = require("./ytnode_wrappers");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YTAPP/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { console.error("YT Application:", x); };
} else {
    __DBG = function( ) { };
}

// This mapping defines how MIME types map onto YT format specifications.
var _MAPPING_MIME_TYPE_TO_FORMAT = {
    "application/json"             : "json",
    "application/x-yt-yson-binary" : "<format=binary>yson",
    "application/x-yt-yson-text"   : "<format=text>yson",
    "application/x-yt-yson-pretty" : "<format=pretty>yson",
    "text/csv"                     : "csv",
    "text/tab-separated-values"    : "dsv",
    "text/x-tskv"                  : "<line_prefix=tskv>dsv"
};

// This mapping defines which HTTP methods various YT data types require.
var _MAPPING_DATA_TYPE_TO_METHOD = {
    "Null"       : "GET",
    "Binary"     : "PUT",
    "Structured" : "POST",
    "Tabular"    : "PUT"
};

////////////////////////////////////////////////////////////////////////////////

function YtCommand(logger, driver, req, rsp) {
    this.logger = logger;
    this.driver = driver;

    this.req = req;
    this.rsp = rsp;

    this.req.parsedUrl = url.parse(this.req.url);
}

YtCommand.prototype.dispatch = function() {
    var self = this;

    utils.callSeq(this, [
        this._prologue,
        this._extractName,
        this._extractParameters,
        this._getInputFormat,
        this._getOutputFormat,
        this._logInformation,
        this._getDescriptor,
        this._addHeaders,
        this._execute,
        this._epilogue
    ], function andThen(err) {
        var thereWasError = err || self.rsp.yt_code !== 0;
        if (thereWasError) {
            var error = err ? err.message : self.rsp.yt_message;

            self.logger.error("Done (failure)", {
                request_id : self.req.uuid,
                error : error
            });

            if (!self.rsp._header) {
                var body = {};

                if (error)               { body.error      = error; }
                if (self.rsp.yt_code)    { body.yt_code    = self.rsp.yt_code; }
                if (self.rsp.yt_message) { body.yt_message = self.rsp.yt_message; }

                body = JSON.stringify(body);

                self.rsp.removeHeader("Transfer-Encoding");
                self.rsp.setHeader("Content-Type", "application/json");
                self.rsp.setHeader("Content-Length", body.length);
                self.rsp.end(body);
            } else {
                self.rsp.end();
            }
        }

        self.logger.info("Done (success)", { request_id : self.req.uuid });
    });
};

YtCommand.prototype._prologue = function(cb) {
    this.rsp.statusCode = 202;
    return cb(null);
};

YtCommand.prototype._epilogue = function(cb) {
    return cb(null);
};

YtCommand.prototype._extractName = function(cb) {
    this.name = this.req.parsedUrl.pathname.slice(1).toLowerCase();
    if (!/^[a-z_]+$/.test(this.name)) {
        this.rsp.statusCode = 400;
        throw new Error("Malformed command name '" + name + "'.");
    }

    return cb(null);
};

YtCommand.prototype._extractParameters = function(cb) {
    this.parameters = utils.numerify(qs.parse(this.req.parsedUrl.query));
    if (!this.parameters) {
        this.rsp.statusCode = 400;
        throw new Error("Unable to parse parameters from the query string.");
    }

    return cb(null);
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
    return cb(null);
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
    return cb(null);
};

YtCommand.prototype._logInformation = function(cb) {
    this.logger.debug("Gathered request parameters", {
        request_id    : this.req.uuid,
        name          : this.name,
        parameters    : this.parameters,
        input_format  : this.input_format,
        output_format : this.output_format
    });
    return cb(null);
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
        request_id            : this.req.uuid,
        descriptor            : this.descriptor,
        input_type_as_string  : input_type_as_string,
        output_type_as_string : output_type_as_string,
        expected_http_method  : expected_http_method,
        actual_http_method    : actual_http_method
    });

    if (expected_http_method != actual_http_method) {
        this.rsp.statusCode = 405;
        this.rsp.setHeader("Allow", expected_http_method);
        throw new Error(
            "Command '" + this.name + "' expects " + input_type_as_string.toLowerCase() +
            " input and hence have to be requested with the " + expected_http_method + " HTTP method.");
    }

    return cb(null);
};

YtCommand.prototype._addHeaders = function(cb) {
    this.rsp.setHeader("Connection", "close");
    this.rsp.setHeader("Transfer-Encoding", "chunked");
    this.rsp.setHeader("Access-Control-Allow-Origin", "*");
    this.rsp.setHeader("Trailer", "X-YT-Response-Code, X-YT-Response-Message");

    return cb(null);
};

YtCommand.prototype._execute = function(cb) {
    var self = this;

    this.driver.execute(this.name,
        this.req, this.input_format,
        this.rsp, this.output_format,
        this.parameters,
        function callback(err, code, message)
        {
            if (err) {
                if (typeof(err) === "string") {
                    self.logger.error(
                        "Command '" + self.name + "' thrown C++ exception",
                        { request_id : self.req.uuid, error : error });
                    return cb(new Error(err));
                }
                if (err instanceof Error) {
                    self.logger.error(
                        "Command '" + self.name + "' failed to execute",
                        { request_id : self.req.uuid, error : error.message });
                    return cb(err);
                }
                return cb(new Error("Unknown error: " + err.toString()));
            } else {
                self.logger.debug(
                    "Command '" + self.name + "' successfully executed",
                    { request_id : self.req.uuid, code : code, error : message });
            }

            self.rsp.yt_code = code;
            self.rsp.yt_message = message;

            if (code === 0) {
                self.rsp.statusCode = 200;
            } else if (code < 0) {
                self.rsp.statusCode = 500;
            } else if (code > 0) {
                self.rsp.statusCode = 400;
            }

            if (code !== 0) {
                self.rsp.addTrailers({
                    "X-YT-Response-Code" : JSON.stringify(code),
                    "X-YT-Response-Message" : JSON.stringify(message)
                });
            }

            return cb(null);
        });
};

////////////////////////////////////////////////////////////////////////////////

function YtEioWatcher(logger, thread_limit) {
    this.logger = logger;
    this.thread_limit = thread_limit;

    __DBG("Eio concurrency: " + thread_limit);

    ytnode_wrappers.SetEioConcurrency(thread_limit);
}

YtEioWatcher.prototype.tackle = function() {
    var info = ytnode_wrappers.GetEioInformation();

    __DBG("Eio information: " + JSON.stringify(info));

    if ((info.nthreads + info.npending < info.nreqs) ||
        (info.nthreads == this.thread_limit && info.nreqs > 0)
        )
    {
        this.logger.info("Eio is saturated; consider increasing thread limit", info);
    }
};

////////////////////////////////////////////////////////////////////////////////

function YtApplication(logger, memory_limit, thread_limit, configuration) {
    var low_watermark = Math.floor(0.25 * memory_limit * 1024 * 1024);
    var high_watermark = Math.ceil(0.95 * memory_limit * 1024 * 1024);

    __DBG("New Application: memory_limit = " + memory_limit);
    __DBG("New Application: thread_limit = " + thread_limit);
    __DBG("New Application: low_watermark = " + low_watermark);
    __DBG("New Application: high_watermark = " + high_watermark);

    var driver = new ytnode_wrappers.YtDriver(configuration, low_watermark, high_watermark);
    var watcher = new YtEioWatcher(logger, thread_limit);

    return function(req, rsp) {
        watcher.tackle();
        return (new YtCommand(logger, driver, req, rsp)).dispatch();
    };
}

function YtLogger(logger) {
    return function(req, rsp, next) {
        req._startTime = new Date();

        if (req._logging) {
            return next();
        } else {
            req._logging = true;
        }

        logger.info("Handling request", {
            request_id  : req.uuid,
            method      : req.method,
            url         : req.originalUrl,
            referrer    : req.headers["referer"] || req.headers["referrer"],
            remote_addr : req.socket && (req.socket.remoteAddress || (req.socket.socket && req.socket.socket.remoteAddress)),
            user_agent  : req.headers["user-agent"]
        });

        var end = rsp.end;
        rsp.end = function(chunk, encoding) {
            rsp.end = end;
            rsp.end(chunk, encoding);

            logger.info("Handled request", {
                request_id   : req.uuid,
                request_time : new Date() - req._startTime,
                status       : rsp.statusCode
            });
        };

        next();
    };
}

function YtAssignRequestId() {
    var buffer = new Buffer(16);
    return function(req, rsp, next) {
        uuid.v4(null, buffer);

        req.uuid = buffer.toString("base64");
        rsp.setHeader("X-YT-Request-Id", req.uuid);

        next();
    };
}

////////////////////////////////////////////////////////////////////////////////

exports.YtApplication = YtApplication;
exports.YtLogger = YtLogger;
exports.YtAssignRequestID = YtAssignRequestId;
