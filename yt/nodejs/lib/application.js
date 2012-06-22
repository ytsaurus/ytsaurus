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
    __DBG.UUID = require("node-uuid");
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

function YtCommand(logger, driver, watcher, req, rsp) {
    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG("Command (" + this.__UUID + ") -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function( ) { };
    }

    this.logger = logger;
    this.driver = driver;
    this.watcher = watcher;

    this.req = req;
    this.rsp = rsp;

    this.req.parsedUrl = url.parse(this.req.url);

    this.__DBG("New");
}

YtCommand.prototype.dispatch = function() {
    this.__DBG("dispatch");

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
        self.__DBG("dispatch -> andThen");

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
    this.__DBG("_prologue");

    this.watcher.tackle();
    this.rsp.statusCode = 202;
    return cb(null);
};

YtCommand.prototype._epilogue = function(cb) {
    this.__DBG("_epilogue");

    this.watcher.tackle();
    return cb(null);
};

YtCommand.prototype._extractName = function(cb) {
    this.__DBG("_extractName");

    this.name = this.req.parsedUrl.pathname.slice(1).toLowerCase();
    if (!/^[a-z_]+$/.test(this.name)) {
        this.rsp.statusCode = 400;
        throw new Error("Malformed command name '" + name + "'.");
    }

    return cb(null);
};

YtCommand.prototype._extractParameters = function(cb) {
    this.__DBG("_extractParameters");

    this.parameters = utils.numerify(qs.parse(this.req.parsedUrl.query));
    if (!this.parameters) {
        this.rsp.statusCode = 400;
        throw new Error("Unable to parse parameters from the query string.");
    }

    return cb(null);
};

YtCommand.prototype._getInputFormat = function(cb) {
    this.__DBG("_getInputFormat");

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
    this.__DBG("_getOutputFormat");

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
    this.__DBG("_logInformation");

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
    this.__DBG("_getDescriptor");

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

    if (this.descriptor.is_heavy && this.watcher.is_choking()) {
        this.rsp.statusCode = 503;
        this.rsp.setHeader("Retry-After", "60");
        throw new Error(
            "Command '" + this.name + "' is heavy and the server is currently overloaded.");
    }

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
    this.__DBG("_addHeaders");

    this.rsp.setHeader("Connection", "close");
    this.rsp.setHeader("Transfer-Encoding", "chunked");
    this.rsp.setHeader("Access-Control-Allow-Origin", "*");
    this.rsp.setHeader("Trailer", "X-YT-Response-Code, X-YT-Response-Message");

    return cb(null);
};

YtCommand.prototype._execute = function(cb) {
    this.__DBG("_execute");

    var self = this;

    this.driver.execute(this.name,
        this.req, this.input_format,
        this.rsp, this.output_format,
        this.parameters,
        function callback(err, code, message)
        {
            self.__DBG("_execute -> (callback)");

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

function YtEioWatcher(logger, thread_limit, spare_threads) {
    this.logger = logger;
    this.thread_limit = thread_limit;
    this.spare_threads = spare_threads;

    __DBG("Eio concurrency: " + thread_limit + " (w/ " + spare_threads + " spare threads)");

    ytnode_wrappers.SetEioConcurrency(thread_limit);
}

YtEioWatcher.prototype.tackle = function() {
    var info = ytnode_wrappers.GetEioInformation();

    __DBG("Eio information: " + JSON.stringify(info));

    if (info.nthreads == this.thread_limit &&
        info.nthreads == info.nreqs && info.nready > 0)
    {
        this.logger.info("Eio is saturated; consider increasing thread limit", info);
    }
};

YtEioWatcher.prototype.is_choking = function() {
    var info = ytnode_wrappers.GetEioInformation();

    if (this.thread_limit - this.spare_threads <= info.nreqs - info.npending) {
        return true;
    } else {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

function YtApplication(logger, configuration) {
    __DBG("Application -> New");

    var low_watermark = parseInt(0.95 * configuration.memory_limit);
    var high_watermark = parseInt(0.80 * configuration.memory_limit);

    __DBG("Application -> low_watermark = " + low_watermark);
    __DBG("Application -> high_watermark = " + high_watermark);

    var driver = new ytnode_wrappers.YtDriver(configuration.driver, low_watermark, high_watermark);
    var watcher = new YtEioWatcher(logger, configuration.thread_limit, configuration.spare_threads);

    return function(req, rsp) {
        return (new YtCommand(
            logger, driver, watcher, req, rsp
        )).dispatch();
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
