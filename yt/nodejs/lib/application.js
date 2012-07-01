var url = require("url");
var crypto = require("crypto");

var buffertools = require("buffertools");
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

// This mapping defines how Content-Encoding and Accept-Encoding map onto YT compressors.
var _MAPPING_STREAM_COMPRESSION = {
    "gzip"     : ytnode_wrappers.ECompression_Gzip,
    "deflate"  : ytnode_wrappers.ECompression_Deflate,
    "x-lzo"    : ytnode_wrappers.ECompression_LZO,
    "x-lzf"    : ytnode_wrappers.ECompression_LZF,
    "x-snappy" : ytnode_wrappers.ECompression_Snappy
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

    this.prematurely_completed = false;

    this.req.parsedUrl = url.parse(this.req.url);

    this.__DBG("New");
}

YtCommand.prototype.dispatch = function() {
    this._prologue();

    utils.callSeq(this, [
        this._getName,
        this._getDescriptor,
        this._checkHttpMethod,
        this._checkHeavy,
        this._getParameters,
        this._getInputFormat,
        this._getInputCompression,
        this._getOutputFormat,
        this._getOutputCompression,
        utils.callIf(this, this._needToCaptureBody,
            this._captureBody,
            this._retainBody),
        this._logRequest,
        this._checkPermissions,
        this._addHeaders,
        this._execute,
    ],
        this._epilogue
    );
};

YtCommand.prototype.dispatchJSON = function(object) {
    var body = JSON.stringify(object);

    this.rsp.removeHeader("Transfer-Encoding");
    this.rsp.removeHeader("Content-Encoding");
    this.rsp.removeHeader("Vary");
    this.rsp.setHeader("Content-Type", "application/json");
    this.rsp.setHeader("Content-Length", body.length);
    this.rsp.writeHead(200);
    this.rsp.end(body);

    this.prematurely_completed = true;
};

YtCommand.prototype._prologue = function() {
    this.__DBG("_prologue");

    this.rsp.statusCode = 202;
};

YtCommand.prototype._epilogue = function(err) {
    this.__DBG("_epilogue");

    this.watcher.tackle();

    var failed = !this.prematurely_completed && (err || this.yt_code !== 0);
    if (failed) {
        var error = err ? err.message : this.yt_message;

        this.logger.error("Done (failure)", {
            request_id : this.req.uuid,
            error : error
        });

        if (!this.rsp._header) {
            var body = {};

            if (error)           { body.error      = error; }
            if (this.yt_code)    { body.yt_code    = this.yt_code; }
            if (this.yt_message) { body.yt_message = this.yt_message; }

            this.dispatchJSON(body);
        } else {
            this.rsp.end();
        }
    }

    this.logger.info("Done (success)", {
        request_id : this.req.uuid,
        bytes_in   : this.bytes_in,
        bytes_out  : this.bytes_out
    });
};

YtCommand.prototype._getName = function(cb) {
    this.__DBG("_getName");

    this.name = this.req.parsedUrl.pathname.slice(1).toLowerCase();

    if (!this.name.length) {
        this.dispatchJSON(this.driver.get_command_descriptors());
        return cb(false);
    }

    if (!/^[a-z_]+$/.test(this.name)) {
        this.rsp.statusCode = 400;
        throw new Error("Malformed command name '" + name + "'.");
    }

    return cb();
};

YtCommand.prototype._getDescriptor = function(cb) {
    this.__DBG("_getDescriptor");

    this.descriptor = this.driver.find_command_descriptor(this.name);

    if (!this.descriptor) {
        this.rsp.statusCode = 404;
        throw new Error("Command '" + this.name + "' is not registered.");
    }

    this.logger.debug("Successfully found command descriptor", {
        request_id : this.req.uuid,
        descriptor : this.descriptor
    });

    return cb();
};

YtCommand.prototype._checkHttpMethod = function(cb) {
    this.__DBG("_checkHttpMethod");

    var expected_http_method, actual_http_method = this.req.method;

    if (this.descriptor.input_type !== ytnode_wrappers.EDataType_Null) {
        expected_http_method = "PUT";
    } else {
        if (this.descriptor.is_volatile) {
            expected_http_method = "POST";
        } else {
            expected_http_method = "GET";
        }
    }

    if (expected_http_method != actual_http_method) {
        this.rsp.statusCode = 405;
        this.rsp.setHeader("Allow", expected_http_method);
        throw new Error(
            "Command '" + this.name + "' have to be executed with the " + expected_http_method + " HTTP method.");
    }

    return cb();
};

YtCommand.prototype._checkHeavy = function(cb) {
    this.__DBG("_checkHeavy");

    if (this.descriptor.is_heavy && this.watcher.is_choking()) {
        this.rsp.statusCode = 503;
        this.rsp.setHeader("Retry-After", "60");
        throw new Error(
            "Command '" + this.name + "' is heavy and the proxy is currently under heavy load. Please, try again later.");
    }

    return cb();
};

YtCommand.prototype._getParameters = function(cb) {
    this.__DBG("_getParameters");

    this.parameters = utils.numerify(qs.parse(this.req.parsedUrl.query));

    if (!this.parameters) {
        this.rsp.statusCode = 400;
        throw new Error("Unable to parse parameters from the query string.");
    }

    return cb();
};

YtCommand.prototype._getInputFormat = function(cb) {
    this.__DBG("_getInputFormat");

    var result, format, header;

    // Firstly, try to deduce input format from Content-Type header.
    header = this.req.headers["content-type"];
    if (typeof(header) === "string") {
        header = header.trim();
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
        result = header.trim();
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        result = "yson";
    }

    this.input_format = result;

    return cb();
};

YtCommand.prototype._getInputCompression = function(cb) {
    this.__DBG("_getInputCompression");

    var result, header;

    header = this.req.headers["content-encoding"];
    if (typeof(header) === "string") {
        header = header.trim();
        result = _MAPPING_STREAM_COMPRESSION[header];
    }

    if (typeof(result) !== "undefined") {
        if (this.req.method !== "PUT") {
            throw new Error("Currently it is only allowed to specify Content-Encoding for PUT requests.");
        } else {
            this.input_compression = result;
        }
    } else {
        this.input_compression = ytnode_wrappers.ECompression_None;
    }

    return cb();
};

// TODO(sandello): 406 Error
YtCommand.prototype._getOutputFormat = function(cb) {
    this.__DBG("_getOutputFormat");

    var result_format, result_mime, header;

    // Firstly, check whether the command produces an octet stream.
    if (this.descriptor.output_type === ytnode_wrappers.EDataType_Binary) {
        // XXX(sandello): This is temporary, until I figure out a better solution.
        this.output_mime = "text/plain";
        this.output_format = "yson";
        return cb();
    }

    // Secondly, try to deduce output format from Accept header.
    header = this.req.headers["accept"];
    if (typeof(header) === "string") {
        for (var mime in _MAPPING_MIME_TYPE_TO_FORMAT) {
            if (utils.accepts(mime, header)) {
                result_mime = mime;
                result_format = _MAPPING_MIME_TYPE_TO_FORMAT[mime];
                break;
            }
        }
    }

    // Thirdly, try to deduce output format from our custom header.
    header = this.req.headers["x-yt-output-format"];
    if (typeof(header) === "string") {
        result_mime = "application/octet-stream";
        result_format = header.trim();
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result_format) === "undefined") {
        result_mime = "text/plain";
        result_format = "<format=pretty;enable_raw=false>yson";
    }

    this.output_mime = result_mime;
    this.output_format = result_format;

    return cb();
};

// TODO(sandello): 415 Error
YtCommand.prototype._getOutputCompression = function(cb) {
    this.__DBG("_getOutputCompression");

    var result_compression, result_mime, header;

    header = this.req.headers["accept-encoding"];
    if (typeof(header) === "string") {
        for (var encoding in _MAPPING_STREAM_COMPRESSION) {
            if (utils.acceptsEncoding(encoding, header)) {
                result_mime = encoding;
                result_compression = _MAPPING_STREAM_COMPRESSION[encoding];
                break;
            }
        }
    }

    if (typeof(result_compression) === "undefined") {
        result_mime = "identity";
        result_compression = ytnode_wrappers.ECompression_None;
    }

    this.output_compression_mime = result_mime;
    this.output_compression = result_compression;

    return cb();
}

YtCommand.prototype._needToCaptureBody = function(cb) {
    this.__DBG("_needToCaptureBody");

    return this.req.method === "POST";
};

YtCommand.prototype._captureBody = function(cb) {
    this.__DBG("_captureBody");

    var self = this;
    var chunks = [];

    this.req.on("data", function(chunk) { chunks.push(chunk); });
    this.req.on("end", function() {
        try {
            var result = buffertools.concat.apply(buffertools, chunks);
            if (result.length) {
                if (self.input_format !== "json") {
                    throw new Error("Currently it is only allowed to use JSON in a POST body.");
                }
                self.parameters = utils.merge(self.parameters, JSON.parse(result));
            }

            self.input_stream = new utils.NullStream();
            self.output_stream = self.rsp;

            return cb();
        } catch (err) {
            return cb(err);
        }
    });
};

YtCommand.prototype._retainBody = function(cb) {
    this.__DBG("_retainBody");

    this.input_stream = this.req;
    this.output_stream = this.rsp;

    return cb();
};

YtCommand.prototype._logRequest = function(cb) {
    this.__DBG("_logRequest");

    this.logger.debug("Gathered request parameters", {
        request_id              : this.req.uuid,
        name                    : this.name,
        parameters              : this.parameters,
        input_format            : this.input_format,
        input_compression       : this.input_compression,
        output_mime             : this.output_mime,
        output_format           : this.output_format,
        output_compression      : this.output_compression,
        output_compression_mime : this.output_compression_mime
    });

    return cb();
};

var RE_HOME    = /^\/\/home|^\/\/"home"|^\/\/\x01\x08\x68\x6f\x6d\x65/;
var RE_TMP     = /^\/\/tmp|^\/\/"tmp"|^\/\/\x01\x06\x74\x6d\x70/;
var RE_STATBOX = /^\/\/statbox|^\/\/"statbox"|^\/\/\x01\x0e\x73\x74\x61\x74\x62\x6f\x78/;

YtCommand.prototype._checkPermissions = function(cb) {
    this.__DBG("_checkPermissions");

    if (this.descriptor.is_volatile) {
        var path = this.parameters.path;
        if (!path || !(RE_HOME.test(path) || RE_TMP.test(path) || RE_STATBOX.test(path))) {
            this.rsp.statusCode = 403;
            throw new Error("Any mutating command is allowed only on //home, //tmp and //statbox");
        }
    }

    return cb();
};

YtCommand.prototype._addHeaders = function(cb) {
    this.__DBG("_addHeaders");

    this.rsp.setHeader("Content-Type", this.output_mime);
    this.rsp.setHeader("Transfer-Encoding", "chunked");
    this.rsp.setHeader("Access-Control-Allow-Origin", "*");
    this.rsp.setHeader("Trailer", "X-YT-Response-Code, X-YT-Response-Message");

    if (this.output_compression !== ytnode_wrappers.ECompression_None) {
        this.rsp.setHeader("Content-Encoding", this.output_compression_mime);
        this.rsp.setHeader("Vary", "Content-Encoding");
    }

    return cb();
};

YtCommand.prototype._execute = function(cb) {
    this.__DBG("_execute");

    var self = this;

    this.driver.execute(this.name,
        this.input_stream, this.input_compression, this.input_format,
        this.output_stream, this.output_compression, this.output_format,
        this.parameters,
        function callback(err, code, message, bytes_in, bytes_out)
        {
            self.__DBG("_execute -> (callback)");

            if (err) {
                if (typeof(err) === "string") {
                    self.logger.error(
                        "Command '" + self.name + "' has thrown C++ exception",
                        { request_id : self.req.uuid, error : error });
                    return cb(new Error(err));
                }
                if (err instanceof Error) {
                    self.logger.error(
                        "Command '" + self.name + "' has failed to execute",
                        { request_id : self.req.uuid, error : error.message });
                    return cb(err);
                }
                return cb(new Error("Unknown error: " + err.toString()));
            } else {
                self.logger.debug(
                    "Command '" + self.name + "' successfully executed",
                    { request_id : self.req.uuid, code : code, error : message });
            }

            self.yt_code = code;
            self.yt_message = message;

            self.bytes_in = bytes_in;
            self.bytes_out = bytes_out;

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

            return cb();
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

function shuffle(array) {
    var i = array.length;
    if (i === 0) {
        return false;
    }
    while (--i) {
        var j = Math.floor(Math.random() * (i + 1));
        var lhs = array[i];
        var rhs = array[j];
        array[i] = rhs;
        array[j] = lhs;
    }
    return array;
}

function YtHostDiscovery(hosts) {
    __DBG("HostDiscovery -> New");

    return function(req, rsp) {
        var body = shuffle(hosts);

        var header = req.headers["accept"];
        if (typeof(header) === "string") {
            /****/ if (utils.accepts("application/json", header)) {
                body = JSON.stringify(body);
                rsp.writeHead(200, {
                    "Content-Length" : body.length,
                    "Content-Type" : "application/json"
                });
            } else if (utils.accepts("text/plain", header)) {
                body = body.toString("\n");
                rsp.writeHead(200, {
                    "Content-Length" : body.length,
                    "Content-Type" : "text/plain"
                });
            } else {
                // Unsupported
            }
        } else {
            body = JSON.stringify(body);
            rsp.writeHead(200, {
                "Content-Length" : body.length,
                "Content-Type" : "application/json"
            });
        }

        rsp.end(body);
    };
}

function YtApplication(logger, configuration) {
    __DBG("Application -> New");

    var low_watermark = parseInt(0.80 * configuration.memory_limit);
    var high_watermark = parseInt(0.95 * configuration.memory_limit);

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

exports.YtHostDiscovery = YtHostDiscovery;
exports.YtApplication = YtApplication;
exports.YtLogger = YtLogger;
exports.YtAssignRequestID = YtAssignRequestId;
