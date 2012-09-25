var url = require("url");
var crypto = require("crypto");

var buffertools = require("buffertools");
var qs = require("qs");

var utils = require("./utils");
var ytnode_wrappers = require("./ytnode_wrappers");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YTAPP/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Command:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function(){};
}

// This mapping defines how MIME types map onto YT format specifications.
var _MIME_TYPE_TO_FORMAT = {
    "application/json"                    : "json",
    "application/x-yamr-delimited"        : "<lenval=false;has_subkey=false>yamr",
    "application/x-yamr-lenval"           : "<lenval=true;has_subkey=false>yamr",
    "application/x-yamr-subkey-delimited" : "<lenval=false;has_subkey=true>yamr",
    "application/x-yamr-subkey-lenval"    : "<lenval=true;has_subkey=true>yamr",
    "application/x-yt-yson-binary"        : "<format=binary>yson",
    "application/x-yt-yson-text"          : "<format=text>yson",
    "application/x-yt-yson-pretty"        : "<format=pretty>yson",
    "text/csv"                            : "<record_separator=\",\";key_value_separator=\":\">dsv",
    "text/tab-separated-values"           : "dsv",
    "text/x-tskv"                         : "<line_prefix=tskv>dsv"
};

// This mapping defines how Content-Encoding and Accept-Encoding map onto YT compressors.
var _STREAM_COMPRESSION = {
    "gzip"     : ytnode_wrappers.ECompression_Gzip,
    "deflate"  : ytnode_wrappers.ECompression_Deflate,
    "x-lzo"    : ytnode_wrappers.ECompression_LZO,
    "x-lzf"    : ytnode_wrappers.ECompression_LZF,
    "x-snappy" : ytnode_wrappers.ECompression_Snappy
};

////////////////////////////////////////////////////////////////////////////////

function YtCommand(logger, driver, watcher, req, rsp) {
    "use strict";
    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG(this.__UUID + " -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function(){};
    }

    this.logger = logger;
    this.driver = driver;
    this.watcher = watcher;

    this.req = req;
    this.rsp = rsp;

    this.prematurely_completed = false;

    this.req.parsedUrl = url.parse(this.req.url);

    this.__DBG("New");

    // This is a total list of class fields; keep this up to date
    // to improve V8 performance (hence JIT relies on preset class properties).
    // See http://blog.mrale.ph/post/14403172501/simple-optimization-checklist/
    // See https://mkw.st/p/gdd11-berlin-v8-performance-tuning-tricks/
    // See http://s3.mrale.ph/nodecamp.eu/
    // See http://v8-io12.appspot.com/index.html
    // See http://floitsch.blogspot.dk/2012/03/optimizing-for-v8-introduction.html
    this.bytes_in_enqueued = 0;
    this.bytes_in_dequeued = 0;
    this.bytes_out_enqueued = 0;
    this.bytes_out_dequeued = 0;
    this.bytes_out = 0;
    this.descriptor = undefined;
    this.input_compression = undefined;
    this.input_format = undefined;
    this.input_stream = undefined;
    this.name = undefined;
    this.output_compression = undefined;
    this.output_compression_mime = undefined;
    this.output_format = undefined;
    this.output_mime = undefined;
    this.output_stream = undefined;
    this.parameters = undefined;
    this.yt_code = undefined;
    this.yt_message = undefined;
}

YtCommand.prototype.dispatch = function() {
    "use strict";
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
        this._execute
    ],
        this._epilogue
    );
};

YtCommand.prototype._dispatchJSON = function(object) {
    "use strict";
    var body = JSON.stringify(object);

    this.rsp.removeHeader("Transfer-Encoding");
    this.rsp.removeHeader("Content-Encoding");
    this.rsp.removeHeader("Vary");
    this.rsp.setHeader("Content-Type", "application/json");
    this.rsp.setHeader("Content-Length", body.length);
    this.rsp.writeHead(this.rsp.statusCode);
    this.rsp.end(body);

    this.prematurely_completed = true;
};

YtCommand.prototype._prologue = function() {
    "use strict";
    this.__DBG("_prologue");

    this.rsp.statusCode = 202;
};

YtCommand.prototype._epilogue = function(err) {
    "use strict";
    this.__DBG("_epilogue");

    this.watcher.tackle();

    var failed = !this.prematurely_completed && (err || this.yt_code !== 0);
    if (failed) {
        var error = err ? err.message : this.yt_message;
        var error_trace = err ? err.stack : undefined;

        this.logger.error("Done (failure)", {
            request_id         : this.req.uuid,
            bytes_in_enqueued  : this.bytes_in_enqueued,
            bytes_in_dequeued  : this.bytes_in_dequeued,
            bytes_out_enqueued : this.bytes_out_enqueued,
            bytes_out_dequeued : this.bytes_out_dequeued,
            error              : error,
            error_trace        : error_trace,
            yt_code            : this.yt_code,
            yt_message         : this.yt_message
        });

        if (!this.rsp._header) {
            var body = {};

            if (!this.rsp.statusCode || (this.rsp.statusCode >= 200 && this.rsp.statusCode < 300)) {
                this.rsp.statusCode = 400;
            }

            if (error)           { body.error       = error; }
            if (error_trace)     { body.error_trace = error_trace;}
            if (this.yt_code)    { body.yt_code     = this.yt_code; }
            if (this.yt_message) { body.yt_message  = this.yt_message; }

            this._dispatchJSON(body);
        } else {
            var trailers = {};

            if (error) {
                trailers["X-YT-Error"] = JSON.stringify(error);
            }
            if (this.yt_code) {
                trailers["X-YT-Response-Code"] = JSON.stringify(this.yt_code);
            }
            if (this.yt_message) {
                trailers["X-YT-Response-Message"] = JSON.stringify(this.yt_message);
            }

            this.rsp.addTrailers(trailers);
            this.rsp.end();
        }
    } else {
        this.logger.info("Done (success)", {
            request_id         : this.req.uuid,
            bytes_in_enqueued  : this.bytes_in_enqueued,
            bytes_in_dequeued  : this.bytes_in_dequeued,
            bytes_out_enqueued : this.bytes_out_enqueued,
            bytes_out_dequeued : this.bytes_out_dequeued
        });
    }
};

YtCommand.prototype._getName = function(cb) {
    "use strict";
    this.__DBG("_getName");

    this.name = this.req.parsedUrl.pathname.slice(1).toLowerCase();

    if (!this.name.length) {
        this._dispatchJSON(this.driver.get_command_descriptors());
        return cb(false);
    }

    if (!/^[a-z_]+$/.test(this.name)) {
        this.rsp.statusCode = 400;
        throw new Error("Malformed command name '" + this.name + "'.");
    }

    return cb();
};

YtCommand.prototype._getDescriptor = function(cb) {
    "use strict";
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
    "use strict";
    this.__DBG("_checkHttpMethod");

    var expected_http_method, actual_http_method = this.req.method;

    if (this.descriptor.input_type_as_integer !== ytnode_wrappers.EDataType_Null) {
        expected_http_method = "PUT";
    } else {
        if (this.descriptor.is_volatile) {
            expected_http_method = "POST";
        } else {
            expected_http_method = "GET";
        }
    }

    if (expected_http_method !== actual_http_method) {
        this.rsp.statusCode = 405;
        this.rsp.setHeader("Allow", expected_http_method);
        throw new Error(
            "Command '" + this.name + "' have to be executed with the " + expected_http_method + " HTTP method.");
    }

    return cb();
};

YtCommand.prototype._checkHeavy = function(cb) {
    "use strict";
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
    "use strict";
    this.__DBG("_getParameters");

    this.parameters = utils.numerify(qs.parse(this.req.parsedUrl.query));

    if (!this.parameters) {
        this.rsp.statusCode = 400;
        throw new Error("Unable to parse parameters from the query string.");
    }

    return cb();
};

YtCommand.prototype._getInputFormat = function(cb) {
    "use strict";
    this.__DBG("_getInputFormat");

    var result, format, header;

    // Firstly, try to deduce input format from Content-Type header.
    header = this.req.headers["content-type"];
    if (typeof(header) === "string") {
        header = header.trim();
        for (var mime in _MIME_TYPE_TO_FORMAT) {
            if (utils.matches(mime, header)) {
                result = _MIME_TYPE_TO_FORMAT[mime];
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
    "use strict";
    this.__DBG("_getInputCompression");

    var result, header;

    header = this.req.headers["content-encoding"];
    if (typeof(header) === "string") {
        header = header.trim();
        result = _STREAM_COMPRESSION[header];
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
    "use strict";
    this.__DBG("_getOutputFormat");

    var result_format, result_mime, header;

    // Firstly, check whether the command produces an octet stream.
    if (this.descriptor.output_type_as_integer === ytnode_wrappers.EDataType_Binary) {
        // XXX(sandello): This is temporary, until I figure out a better solution.
        this.output_mime = "text/plain";
        this.output_format = "yson";
        return cb();
    }

    // Secondly, try to deduce output format from Accept header.
    header = this.req.headers["accept"];
    if (typeof(header) === "string") {
        if (header.indexOf("*") === -1) {
            if (_MIME_TYPE_TO_FORMAT.hasOwnProperty(header)) {
                result_mime = header;
                result_format = _MIME_TYPE_TO_FORMAT[header];
            }
        } else {
            for (var mime in _MIME_TYPE_TO_FORMAT) {
                if (utils.acceptsType(mime, header)) {
                    result_mime = mime;
                    result_format = _MIME_TYPE_TO_FORMAT[mime];
                    break;
                }
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
    "use strict";
    this.__DBG("_getOutputCompression");

    var result_compression, result_mime, header;

    header = this.req.headers["accept-encoding"];
    if (typeof(header) === "string") {
        for (var encoding in _STREAM_COMPRESSION) {
            if (utils.acceptsEncoding(encoding, header)) {
                result_mime = encoding;
                result_compression = _STREAM_COMPRESSION[encoding];
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
};

YtCommand.prototype._needToCaptureBody = function(cb) {
    "use strict";
    this.__DBG("_needToCaptureBody");

    return this.req.method === "POST";
};

YtCommand.prototype._captureBody = function(cb) {
    "use strict";
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
    "use strict";
    this.__DBG("_retainBody");

    this.input_stream = this.req;
    this.output_stream = this.rsp;

    return cb();
};

YtCommand.prototype._logRequest = function(cb) {
    "use strict";
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
var RE_MAPS    = /^\/\/maps|^\/\/"maps"/;
var RE_STATBOX = /^\/\/statbox|^\/\/"statbox"|^\/\/\x01\x0e\x73\x74\x61\x74\x62\x6f\x78/;

YtCommand.prototype._checkPermissions = function(cb) {
    "use strict";
    this.__DBG("_checkPermissions");

    if (this.descriptor.is_volatile) {
        var self = this;
        var paths = [];

        // Collect all paths mentioned within a request.
        // This is an approximation, but a decent one.
        try {
            if (typeof(this.parameters.path) === "string") {
                paths.push(this.parameters.path);
            }
        } catch(err) {
        }

        try {
            if (typeof(this.parameters.spec.input_table_path) === "string") {
                paths.push(this.parameters.spec.input_table_path);
            }
        } catch(err) {
        }

        try {
            this.parameters.spec.input_table_paths.forEach(function(path) {
                if (typeof(path) === "string") { paths.push(path); }
            });
        } catch(err) {
        }

        try {
            if (typeof(this.parameters.spec.output_table_path) === "string") {
                paths.push(this.parameters.spec.output_table_path);
            }
        } catch(err) {
        }

        try {
            this.parameters.spec.output_table_paths.forEach(function(path) {
                if (typeof(path) === "string") { paths.push(path); }
            });
        } catch(err) {
        }

        paths.forEach(function(path) {
            if (!(RE_HOME.test(path) || RE_TMP.test(path) || RE_MAPS.test(path) || RE_STATBOX.test(path))) {
                self.rsp.statusCode = 403;
                throw new Error("Any mutating command is allowed only on //home, //tmp and //statbox");
            }
        });
    }

    return cb();
};

YtCommand.prototype._addHeaders = function(cb) {
    "use strict";
    this.__DBG("_addHeaders");

    this.rsp.setHeader("Content-Type", this.output_mime);
    this.rsp.setHeader("Transfer-Encoding", "chunked");
    this.rsp.setHeader("Access-Control-Allow-Origin", "*");
    this.rsp.setHeader("Trailer", "X-YT-Error, X-YT-Response-Code, X-YT-Response-Message");

    if (this.output_compression !== ytnode_wrappers.ECompression_None) {
        this.rsp.setHeader("Content-Encoding", this.output_compression_mime);
        this.rsp.setHeader("Vary", "Content-Encoding");
    }

    return cb();
};

YtCommand.prototype._execute = function(cb) {
    "use strict";
    this.__DBG("_execute");

    var self = this;

    this.driver.execute(this.name,
        this.input_stream, this.input_compression, this.input_format,
        this.output_stream, this.output_compression, this.output_format,
        this.parameters,
        function callback(err, code, message, bytes_in_enqueued, bytes_in_dequeued, bytes_out_enqueued, bytes_out_dequeued)
        {
            self.__DBG("_execute -> (callback)");

            if (err) {
                if (typeof(err) === "string") {
                    self.logger.error(
                        "Command '" + self.name + "' has thrown C++ exception",
                        { request_id : self.req.uuid, error : err });
                    return cb(new Error(err));
                }
                if (err instanceof Error) {
                    self.logger.error(
                        "Command '" + self.name + "' has failed to execute",
                        { request_id : self.req.uuid, error : err.message });
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

            self.bytes_in_enqueued = bytes_in_enqueued;
            self.bytes_in_dequeued = bytes_in_dequeued;
            self.bytes_out_enqueued = bytes_out_enqueued;
            self.bytes_out_dequeued = bytes_out_dequeued;

            if (code === 0) {
                self.rsp.statusCode = 200;
            } else if (code < 0) {
                self.rsp.statusCode = 500;
            } else if (code > 0) {
                self.rsp.statusCode = 400;
            }

            return cb();
        });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtCommand;
