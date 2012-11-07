var url = require("url");

var buffertools = require("buffertools");
var qs = require("qs");

var Q = require("q");

var utils = require("./utils");
var binding = require("./ytnode");

var YtError = require("./error").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT(ALL|APP)/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Command:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function(){};
}

// This mapping defines how MIME types map onto YT format specifications.
var _MIME_TO_FORMAT = {
    "application/json" : new binding.TNodeJSNode({
        $value : "json"
    }),
    "application/x-yamr-delimited" : new binding.TNodeJSNode({
        $attributes : { lenval : false, has_subkey : false },
        $value : "yamr",
    }),
    "application/x-yamr-lenval" : new binding.TNodeJSNode({
        $attributes : { lenval : true, has_subkey : false },
        $value : "yamr"
    }),
    "application/x-yamr-subkey-delimited" : new binding.TNodeJSNode({
        $attributes : { lenval : false, has_subkey : true },
        $value : "yamr"
    }),
    "application/x-yamr-subkey-lenval" : new binding.TNodeJSNode({
        $attributes : { lenval : true, has_subkey : true },
        $value : "yamr"
    }),
    "application/x-yt-yson-binary" : new binding.TNodeJSNode({
        $attributes : { format : "binary" },
        $value : "yson"
    }),
    "application/x-yt-yson-pretty" : new binding.TNodeJSNode({
        $attributes : { format : "pretty" },
        $value : "yson"
    }),
    "application/x-yt-yson-text" : new binding.TNodeJSNode({
        $attributes : { format : "text" },
        $value : "yson"
    }),
    "text/csv" : new binding.TNodeJSNode({
        $attributes : { record_separator : ",", key_value_separator : ":" },
        $value : "dsv"
    }),
    "text/tab-separated-values" : new binding.TNodeJSNode({
        $value : "dsv"
    }),
    "text/x-tskv" : new binding.TNodeJSNode({
        $attributes : { line_prefix : "tskv" },
        $value : "dsv"
    })
};

// This mapping defines which MIME types could be used to encode specific data type.
var _MIME_BY_OUTPUT_TYPE = {};
_MIME_BY_OUTPUT_TYPE[binding.EDataType_Structured] = [
    "application/json",
    "application/x-yt-yson-pretty",
    "application/x-yt-yson-text",
    "application/x-yt-yson-binary"
];
_MIME_BY_OUTPUT_TYPE[binding.EDataType_Tabular] = [
    "application/x-yamr-delimited",
    "application/x-yamr-lenval",
    "application/x-yamr-subkey-delimited",
    "application/x-yamr-subkey-lenval",
    "application/x-yt-yson-binary",
    "application/x-yt-yson-text",
    "application/x-yt-yson-pretty",
    "text/csv",
    "text/tab-separated-values",
    "text/x-tskv"
];

// This mapping defines how Content-Encoding and Accept-Encoding map onto YT compressors.
var _ENCODING_TO_COMPRESSION = {
    "gzip"     : binding.ECompression_Gzip,
    "deflate"  : binding.ECompression_Deflate,
    "identity" : binding.ECompression_None
};

var _ENCODING_ALL = [ "gzip", "deflate", "identity" ];

var _PREDEFINED_JSON_FORMAT = new binding.TNodeJSNode({ $value : "json" });
var _PREDEFINED_YSON_FORMAT = new binding.TNodeJSNode({ $value : "yson" });

// === HTTP Status Codes
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
// === V8 Optimization
// http://blog.mrale.ph/post/14403172501/simple-optimization-checklist/
// https://mkw.st/p/gdd11-berlin-v8-performance-tuning-tricks/
// http://s3.mrale.ph/nodecamp.eu/
// http://v8-io12.appspot.com/index.html
// http://floitsch.blogspot.dk/2012/03/optimizing-for-v8-introduction.html

////////////////////////////////////////////////////////////////////////////////

function YtCommand(logger, driver, watcher, read_only, pause, req, rsp) {
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
    this.read_only = read_only;
    this.pause = pause;

    this.req = req;
    this.rsp = rsp;

    this.__DBG("New");

    // This is a total list of class fields; keep this up to date to improve V8
    // performance (JIT code reuse depends on so-called "hidden class"; see links
    // on V8 optimization for more details).

    this.name = undefined;
    this.parameters = undefined;
    this.result = undefined;
    this.descriptor = undefined;

    this.input_compression = undefined;
    this.input_format = undefined;
    this.input_stream = undefined;

    this.output_compression = undefined;
    this.output_format = undefined;
    this.output_stream = undefined;

    this.mime_compression = undefined;
    this.mime_type = undefined;

    this.bytes_in = undefined;
    this.bytes_out = undefined;
}

YtCommand.prototype.dispatch = function() {
    "use strict";
    this.__DBG("dispatch");

    var self = this;

    self.req.parsedUrl = url.parse(self.req.url);
    self.rsp.statusCode = 202; // "Accepted". This may change during the pipeline.

    Q
        .fcall(function() {
            self._getName();
            self._getDescriptor();
            self._checkHttpMethod();
            self._checkReadOnlyAndHeavy();
            self._getInputFormat();
            self._getInputCompression();
            self._getOutputFormat();
            self._getOutputCompression();
        })
        .then(self._captureParameters.bind(self))
        .then(function() {
            self._logRequest();
            self._checkPermissions();
            self._addHeaders();
        })
        .then(self._execute.bind(self))
        .fail(function(err) {
            return YtError.ensureWrapped(
                err,
                "Unhandled error in command pipeline");
        })
        .then(self._epilogue.bind(self))
        .end();
};

YtCommand.prototype._dispatchAsJson = function(body) {
    "use strict";
    this.__DBG("_dispatchAsJson");

    this.rsp.removeHeader("Transfer-Encoding");
    this.rsp.removeHeader("Content-Encoding");
    this.rsp.removeHeader("Vary");
    this.rsp.setHeader("Content-Type", "application/json");
    this.rsp.setHeader("Content-Length", body.length);
    this.rsp.writeHead(this.rsp.statusCode);
    this.rsp.end(body);
};

YtCommand.prototype._epilogue = function(err) {
    "use strict";
    this.__DBG("_epilogue");

    var sent_headers = !!this.rsp._header;

    if (!sent_headers) {
        this.rsp.removeHeader("Trailer");
        this.rsp.addHeader("X-YT-Error", err.toJson());
    } else {
        this.rsp.addTrailers({
            "X-YT-Error" : err.toJson(),
            "X-YT-Response-Code" : err.getCode(),
            "X-YT-Response-Message" : err.getMessage()
        });
    }

    if (err.getCode()) {
        this.logger.error("Done (failure)", {
            request_id : this.req.uuid,
            bytes_in   : this.bytes_in,
            bytes_out  : this.bytes_out,
            error      : err,
        });

        if (!sent_headers) {
            if (!this.rsp.statusCode ||
                (this.rsp.statusCode >= 200 && this.rsp.statusCode < 300))
            {
                this.rsp.statusCode = 400;
            }
            this._dispatchAsJson(err.toJson());
        }
    } else {
        this.logger.info("Done (success)", {
            request_id : this.req.uuid,
            bytes_in   : this.bytes_in,
            bytes_out  : this.bytes_out,
            error      : err
        });

        if (!sent_headers) {
            this.rsp.statusCode = 200;
        }
    }

    this.rsp.end();
};

YtCommand.prototype._getName = function() {
    "use strict";
    this.__DBG("_getName");

    this.name = this.req.parsedUrl.pathname.slice(1).toLowerCase();

    if (!this.name.length) {
        // Bail out to API description.
        this.rsp.statusCode = 200;
        this.rsp.setHeader("Access-Control-Allow-Origin", "*");
        this._dispatchAsJson(JSON.stringify(this.driver.get_command_descriptors()));
        throw new YtError();
    }

    if (!/^[a-z_]+$/.test(this.name)) {
        throw new YtError("Malformed command name " + JSON.stringify(this.name) + ".");
    }
};

YtCommand.prototype._getDescriptor = function() {
    "use strict";
    this.__DBG("_getDescriptor");

    this.descriptor = this.driver.find_command_descriptor(this.name);

    if (!this.descriptor) {
        this.rsp.statusCode = 404;
        throw new YtError("Command '" + this.name + "' is not registered.");
    }
};

YtCommand.prototype._checkHttpMethod = function() {
    "use strict";
    this.__DBG("_checkHttpMethod");

    var expected_http_method, actual_http_method = this.req.method;

    if (this.descriptor.input_type_as_integer !== binding.EDataType_Null) {
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
        throw new YtError(
            "Command '" + this.name +
            "' have to be executed with the " +
            expected_http_method +
            " HTTP method while the actual one is " +
            actual_http_method +
            ".");
    }
};

YtCommand.prototype._checkReadOnlyAndHeavy = function() {
    "use strict";
    this.__DBG("_checkHeavy");

    if (this.descriptor.is_volatile && this.read_only) {
        this.rsp.statusCode = 503;
        this.rsp.setHeader("Retry-After", "60");
        throw new YtError(
            "Command '" + this.name + "' is volatile and the proxy is in read-only mode.");
    }

    if (this.descriptor.is_heavy && this.watcher.is_choking()) {
        this.rsp.statusCode = 503;
        this.rsp.setHeader("Retry-After", "60");
        throw new YtError(
            "Command '" + this.name +
            "' is heavy and the proxy is currently under heavy load. " +
            "Please, try another proxy or try again later.");
    }
};

YtCommand.prototype._getInputFormat = function() {
    "use strict";
    this.__DBG("_getInputFormat");

    var result, header;

    // Firstly, try to deduce input format from Content-Type header.
    header = this.req.headers["content-type"];
    if (typeof(header) === "string") {
        header = header.trim();
        for (var mime in _MIME_TO_FORMAT) {
            if (_MIME_TO_FORMAT.hasOwnProperty(mime) && utils.matches(mime, header)) {
                result = _MIME_TO_FORMAT[mime];
                break;
            }
        }
    }

    // Secondly, try to deduce output format from our custom header.
    header = this.req.headers["x-yt-input-format"];
    if (typeof(header) === "string") {
        try {
            result = new binding.TNodeJSNode(
                header.trim(),
                binding.ECompression_None,
                _PREDEFINED_JSON_FORMAT);
        } catch(err) {
            throw new YtError("Unable to parse X-YT-Input-Format header.", err);
        }
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        result = _PREDEFINED_YSON_FORMAT;
    }

    this.input_format = result;
};

YtCommand.prototype._getInputCompression = function() {
    "use strict";
    this.__DBG("_getInputCompression");

    var result, header;

    header = this.req.headers["content-encoding"];
    if (typeof(header) === "string") {
        header = header.trim();
        if (_ENCODING_TO_COMPRESSION.hasOwnProperty(header)) {
            result = _ENCODING_TO_COMPRESSION[header];
        } else {
            throw new YtError("Unsupported Content-Encoding " + JSON.stringify(header) + ".");
        }
    }

    if (typeof(result) === "undefined") {
        result = binding.ECompression_None;
    }

    this.input_compression = result;
};

YtCommand.prototype._getOutputFormat = function() {
    "use strict";
    this.__DBG("_getOutputFormat");

    var result_format, result_mime, header;

    // Firstly, check whether the command either produces no data or an octet stream.
    if (this.descriptor.output_type_as_integer === binding.EDataType_Null) {
        this.output_format = _PREDEFINED_YSON_FORMAT;
        this.mime_type = undefined;
        return;
    }
    if (this.descriptor.output_type_as_integer === binding.EDataType_Binary) {
        // TODO(sandello): Replace with application/octet-stream and Content-Disposition.
        this.output_format = _PREDEFINED_YSON_FORMAT;
        this.mime_type = "text/plain";
        return;
    }

    // Secondly, try to deduce output format from Accept header.
    header = this.req.headers["accept"];
    if (typeof(header) === "string") {
        result_mime = utils.bestAcceptedType(
            _MIME_BY_OUTPUT_TYPE[this.descriptor.output_type_as_integer],
            header);

        if (!result_mime) {
            this.rsp.statusCode = 406;
            throw new YtError("Could not determine feasible Content-Type given Accept constraints.");
        }

        result_format = _MIME_TO_FORMAT[result_mime];
    }

    // Thirdly, try to deduce output format from our custom header.
    header = this.req.headers["x-yt-output-format"];
    if (typeof(header) === "string") {
        try {
            result_mime = "application/octet-stream";
            result_format = new binding.TNodeJSNode(
                header.trim(),
                binding.ECompression_None,
                _PREDEFINED_JSON_FORMAT);
        } catch(err) {
            throw new YtError("Unable to parse X-YT-Output-Format header.", err);
        }
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result_format) === "undefined") {
        result_mime = "text/plain";
        result_format = _PREDEFINED_YSON_FORMAT;
    }

    this.output_format = result_format;
    this.mime_type = result_mime;
};

YtCommand.prototype._getOutputCompression = function() {
    "use strict";
    this.__DBG("_getOutputCompression");

    var result_compression, result_mime, header;

    header = this.req.headers["accept-encoding"];
    if (typeof(header) === "string") {
        result_mime = utils.bestAcceptedEncoding(
            _ENCODING_ALL,
            header);

        if (!result_mime) {
            this.rsp.statusCode = 415;
            throw new YtError("Could not determine feasible Content-Encoding given Accept-Encoding constraints.");
        }

        result_compression = _ENCODING_TO_COMPRESSION[result_mime];
    }

    if (typeof(result_compression) === "undefined") {
        result_mime = "identity";
        result_compression = binding.ECompression_None;
    }

    this.output_compression = result_compression;
    this.mime_compression = result_mime;
};

YtCommand.prototype._captureParameters = function() {
    "use strict";
    this.__DBG("_captureParameters");

    var header;
    var parameters_from_url, parameters_from_header, parameters_from_body;

    try {
        parameters_from_url = utils.numerify(qs.parse(this.req.parsedUrl.query));
        parameters_from_url = new binding.TNodeJSNode(parameters_from_url);
    } catch(err) {
        throw new YtError("Unable to parse parameters from the query string.", err);
    }

    try {
        header = this.req.headers["x-yt-parameters"];
        if (typeof(header) === "string") {
            parameters_from_header = new binding.TNodeJSNode(
                header,
                binding.ECompression_None,
                _PREDEFINED_JSON_FORMAT);
        }
    } catch(err) {
        throw new YtError("Unable to parse parameters from the request header X-YT-Parameters.", err);
    }

    if (this.req.method === "POST") {
        // Here we heavily rely on fact that HTTP method was checked beforehand.
        // Moreover, there is a convention that mutating commands with structured input
        // are served with POST method (see |_checkHttpMethod|).
        parameters_from_body = this._captureBody();
        this.input_stream = new utils.NullStream();
        this.output_stream = this.rsp;
        this.pause = utils.Pause(this.input_stream);
    } else {
        parameters_from_body = Q.resolve();
        this.input_stream = this.req;
        this.output_stream = this.rsp;
    }

    var self = this;

    return Q
        .all([ parameters_from_url, parameters_from_header, parameters_from_body ])
        .spread(function(from_url, from_header, from_body) {
            self.parameters = binding.CreateMergedNode(from_url, from_header, from_body);
        });
};

YtCommand.prototype._captureBody = function() {
    "use strict";
    this.__DBG("_captureBody");

    var deferred = Q.defer();

    var self = this;
    var chunks = [];

    this.req.on("data", function(chunk) { chunks.push(chunk); });
    this.req.on("end", function() {
        try {
            var body = buffertools.concat.apply(undefined, chunks);
            if (body.length) {
                deferred.resolve(new binding.TNodeJSNode(
                    body,
                    self.input_compression,
                    self.input_format));
            } else {
                deferred.resolve();
            }
        } catch (err) {
            deferred.reject(new YtError(
                "Unable to parse parameters from the request body.",
                err));
        }
    });

    this.pause.unpause();

    return deferred.promise;
};

YtCommand.prototype._logRequest = function() {
    "use strict";
    this.__DBG("_logRequest");

    this.logger.debug("Gathered request parameters", {
        request_id              : this.req.uuid,
        name                    : this.name,
        parameters              : this.parameters.Print(),
        input_format            : this.input_format.Print(),
        input_compression       : this.input_compression,
        output_format           : this.output_format.Print(),
        output_compression      : this.output_compression
    });
};

var RE_WRITABLE    = /^\/\/home|^\/\/tmp|^\/\/maps|^\/\/statbox/;

YtCommand.prototype._checkPermissions = function() {
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
            if (!RE_WRITABLE.test(path)) {
                self.rsp.statusCode = 403;
                throw new YtError("Any mutating command is allowed only on //home, //tmp, //statbox and //maps. Violating path was: " + JSON.stringify(path));
            }
        });
    }
};

YtCommand.prototype._addHeaders = function() {
    "use strict";
    this.__DBG("_addHeaders");

    if (this.mime_type) {
        this.rsp.setHeader("Content-Type", this.mime_type);
    }

    if (this.output_compression !== binding.ECompression_None) {
        this.rsp.setHeader("Content-Encoding", this.mime_compression);
        this.rsp.setHeader("Vary", "Content-Encoding");
    }

    this.rsp.setHeader("Transfer-Encoding", "chunked");
    this.rsp.setHeader("Access-Control-Allow-Origin", "*");
    this.rsp.setHeader("Trailer", "X-YT-Error, X-YT-Response-Code, X-YT-Response-Message");
};

YtCommand.prototype._execute = function(cb) {
    "use strict";
    this.__DBG("_execute");

    var self = this;

    process.nextTick(function() { self.pause.unpause(); });

    return this.driver.execute(this.name,
        this.input_stream, this.input_compression, this.input_format,
        this.output_stream, this.output_compression, this.output_format,
        this.parameters).then(
        function(args) {
            var response = args[0];

            self.logger.error(
                "Command '" + self.name + "' successfully executed",
                { request_id : self.req.uuid, response : response });

            self.result    = response;
            self.bytes_in  = args[1];
            self.bytes_out = args[2];

            if (response.code === 0) {
                self.rsp.statusCode = 200;
            } else if (response.code < 0) {
                self.rsp.statusCode = 500;
            } else if (response.code > 0) {
                self.rsp.statusCode = 400;
            }

            return response;
        },
        function(err) {
            self.logger.error(
                "Command '" + self.name + "' has failed to execute",
                { request_id : self.req.uuid, response : err });

            return new YtError("Unexpected error while calling IDriver::Execute()", err);
        });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtCommand;
