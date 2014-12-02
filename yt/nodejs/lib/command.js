var url = require("url");

var buffertools = require("buffertools");
var qs = require("qs");

var Q = require("bluebird");

var utils = require("./utils");
var binding = require("./ytnode");

var YtError = require("./error").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("C", "Command");

// This mapping defines the supported API versions.
var _VERSION_TO_FACADE = {
    "v2": require("./driver_facade_v2").that,
    "v3": require("./driver_facade_v3").that,
};

// This mapping defines how MIME types map onto YT format specifications.
var _MIME_TO_FORMAT = {
    "application/json": binding.CreateV8Node({
        $value: "json"
    }),
    "application/x-yamr-delimited": binding.CreateV8Node({
        $attributes: { lenval: false, has_subkey: false },
        $value: "yamr",
    }),
    "application/x-yamr-lenval": binding.CreateV8Node({
        $attributes: { lenval: true, has_subkey: false },
        $value: "yamr"
    }),
    "application/x-yamr-subkey-delimited": binding.CreateV8Node({
        $attributes: { lenval: false, has_subkey: true },
        $value: "yamr"
    }),
    "application/x-yamr-subkey-lenval": binding.CreateV8Node({
        $attributes: { lenval: true, has_subkey: true },
        $value: "yamr"
    }),
    "application/x-yt-yson-binary": binding.CreateV8Node({
        $attributes: { format: "binary" },
        $value: "yson"
    }),
    "application/x-yt-yson-pretty": binding.CreateV8Node({
        $attributes: { format: "pretty" },
        $value: "yson"
    }),
    "application/x-yt-yson-text": binding.CreateV8Node({
        $attributes: { format: "text" },
        $value: "yson"
    }),
    "text/csv": binding.CreateV8Node({
        $attributes: { record_separator: ",", key_value_separator: ":" },
        $value: "dsv"
    }),
    "text/tab-separated-values": binding.CreateV8Node({
        $value: "dsv"
    }),
    "text/x-tskv": binding.CreateV8Node({
        $attributes: { line_prefix: "tskv" },
        $value: "dsv"
    }),
};

// This mapping defines which MIME types could be used to encode specific data type.
var _MIME_BY_OUTPUT_TYPE = {};
_MIME_BY_OUTPUT_TYPE[binding.EDataType_Structured] = [
    "application/json",
    "application/x-yt-yson-pretty",
    "application/x-yt-yson-text",
    "application/x-yt-yson-binary",
];
_MIME_BY_OUTPUT_TYPE[binding.EDataType_Tabular] = [
    "application/json",
    "application/x-yamr-delimited",
    "application/x-yamr-lenval",
    "application/x-yamr-subkey-delimited",
    "application/x-yamr-subkey-lenval",
    "application/x-yt-yson-binary",
    "application/x-yt-yson-text",
    "application/x-yt-yson-pretty",
    "text/csv",
    "text/tab-separated-values",
    "text/x-tskv",
];

// This mapping defines default output formats for data types.
var _MIME_DEFAULT = {};
_MIME_DEFAULT[binding.EDataType_Structured] = "application/json";
_MIME_DEFAULT[binding.EDataType_Tabular] = "application/x-yt-yson-text";

// This mapping defines how Content-Encoding and Accept-Encoding map onto YT compressors.
var _ENCODING_TO_COMPRESSION = {
    "gzip"     : binding.ECompression_Gzip,
    "deflate"  : binding.ECompression_Deflate,
    "x-lzop"   : binding.ECompression_LZOP,
    "y-lzo"    : binding.ECompression_LZO,
    "y-lzf"    : binding.ECompression_LZF,
    "y-snappy" : binding.ECompression_Snappy,
    "identity" : binding.ECompression_None,
};

var _ENCODING_ALL = Object.keys(_ENCODING_TO_COMPRESSION);

var _PREDEFINED_JSON_FORMAT = binding.CreateV8Node("json");
var _PREDEFINED_YSON_FORMAT = binding.CreateV8Node("yson");

// === HTTP Status Codes
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
// === V8 Optimization
// http://blog.mrale.ph/post/14403172501/simple-optimization-checklist/
// https://mkw.st/p/gdd11-berlin-v8-performance-tuning-tricks/
// http://s3.mrale.ph/nodecamp.eu/
// http://v8-io12.appspot.com/index.html
// http://floitsch.blogspot.dk/2012/03/optimizing-for-v8-introduction.html

////////////////////////////////////////////////////////////////////////////////

function YtCommand(logger, driver, coordinator, watcher, rate_check_cache, pause) {
    "use strict";
    this.__DBG = __DBG.Tagged();

    this.logger = logger;
    this.driver = driver;
    this.coordinator = coordinator;
    this.watcher = watcher;
    this.rate_check_cache = rate_check_cache;
    this.pause = pause;

    // This is a total list of class fields; keep this up to date to improve V8
    // performance (JIT code reuse depends on so-called "hidden class"; see links
    // on V8 optimization for more details).

    this.req = null;
    this.rsp = null;

    this.omit_trailers = undefined;
    this.request_id = undefined;

    this.name = undefined;
    this.user = undefined;

    this.parameters = undefined;
    this.response_parameters = binding.CreateV8Node({});

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

    this.__DBG("New");
}

YtCommand.prototype.dispatch = function(req, rsp) {
    "use strict";
    this.__DBG("dispatch");

    var self = this;

    self.req = req;
    self.rsp = rsp;

    self.request_id = self.req.uuid_ui64;

    self.rsp.statusCode = 200;

    Q
        .try(function() {
            self._parseRequest();
            self._getName();
            self._getUser();
            self._getDescriptor();
            self._checkHttpMethod();
            self._checkAvailability();
            self._redirectHeavyRequests();
            self._getInputFormat();
            self._getInputCompression();
            self._getOutputFormat();
            self._getOutputCompression();
        })
        .then(self._captureParameters.bind(self))
        .then(function() {
            self._logRequest();
            self._addHeaders();
        })
        .then(self._execute.bind(self))
        .catch(function(err) {
            return YtError.ensureWrapped(
                err,
                "Unhandled error in the command pipeline");
        })
        .then(self._epilogue.bind(self))
        .done();
};

YtCommand.prototype._epilogue = function(result) {
    "use strict";
    this.__DBG("_epilogue");

    var extra_headers = {
        "X-YT-Error": result.toJson(),
        "X-YT-Response-Code": utils.escapeHeader(result.getCode()),
        "X-YT-Response-Message": utils.escapeHeader(result.getMessage()),
        "X-YT-Response-Parameters": this.response_parameters.Print(binding.ECompression_None, _PREDEFINED_JSON_FORMAT),
    };
    var sent_headers = !!this.rsp._header;
    if (!sent_headers) {
        this.rsp.removeHeader("Trailer");
        for (var p in extra_headers) {
            this.rsp.setHeader(p, extra_headers[p]);
        }
    } else if (!this.omit_trailers) {
        this.rsp.addTrailers(extra_headers);
    }

    this.logger.debug("Done (" + (result.isOK() ? "success" : "failure") + ")", {
        bytes_in: this.bytes_in,
        bytes_out: this.bytes_out,
        result: result,
    });

    if (!result.isOK()) {
        if (result.isUserBanned() || result.isRequestRateLimitExceeded()) {
            this.logger.debug("User '" + this.user + "' was banned or has hit rate limit");
            this.rate_check_cache.set(this.user, result.toJson());
        }

        if (!sent_headers) {
            if (!this.rsp.statusCode ||
                (this.rsp.statusCode >= 200 && this.rsp.statusCode < 300))
            {
                var isServerSide = result.isUnavailable() || result.isAllTargetNodesFailed();
                this.rsp.statusCode = isServerSide ? 503 : 400;
                this.rsp.setHeader("Retry-After", "60");
            }
            utils.dispatchAs(
                this.rsp,
                result.toJson(),
                "application/json");
        }
    }

    this.rsp.end();
};

YtCommand.prototype._parseRequest = function() {
    "use strict";
    this.__DBG("_parseRequest");

    this.req.parsedUrl = url.parse(this.req.url);
    this.req.parsedQuery = qs.parse(this.req.parsedUrl.query);

    // Do not care about 'null' or 'undefined' in code below.
    var ua = this.req.headers["user-agent"] + "";
    var is_ie = ua.indexOf("Trident") !== -1;

    // XXX(sandello): IE is bugged; it fails to parse request with trailing
    // headers that include colons. Remarkable.
    this.omit_trailers = is_ie;
};

YtCommand.prototype._getName = function() {
    "use strict";
    this.__DBG("_getName");

    var versioned_name = this.req.parsedUrl.pathname.slice(1).toLowerCase();

    var version;
    var name;
    var facade;
    var driver;

    if (!versioned_name.length) {
        utils.dispatchJson(this.rsp, Object.keys(_VERSION_TO_FACADE));
        throw new YtError();
    }

    var p = versioned_name.indexOf("/");
    if (p === -1) {
        if (/^v[0-9]+$/.test(versioned_name)) {
            version = versioned_name;
            name = "";
        } else {
            throw new YtError("Unspecified API version");
        }
    } else {
        version = versioned_name.substr(0, p);
        name = versioned_name.substr(p + 1);
    }

    if (!_VERSION_TO_FACADE.hasOwnProperty(version)) {
        throw new YtError("Unsupported API version " + JSON.stringify(version) + ".");
    }

    facade = _VERSION_TO_FACADE[version];
    driver = facade(this.driver);

    if (!name.length) {
        // Bail out to API description.
        utils.dispatchJson(
            this.rsp,
            driver.get_command_descriptors());
        throw new YtError();
    }

    if (!/^[a-z_]+$/.test(name)) {
        throw new YtError("Malformed command name " + JSON.stringify(name) + ".");
    }

    this.name = name;
    this.driver = driver;
};

YtCommand.prototype._getUser = function() {
    "use strict";
    this.__DBG("_getUser");

    if (typeof(this.req.authenticated_user) === "string") {
        this.user = this.req.authenticated_user;
    } else {
        throw new YtError("Failed to identify user credentials.");
    }

    var rate_check_result = this.rate_check_cache.get(this.user);
    if (typeof(rate_check_result) !== "undefined") {
        this.rsp.statusCode = 429;
        utils.dispatchAs(this.rsp, rate_check_result, "application/json");
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

    if (actual_http_method !== expected_http_method) {
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

YtCommand.prototype._checkAvailability = function() {
    "use strict";
    this.__DBG("_checkAvailability");

    if (this.coordinator.getSelf().banned) {
        this.rsp.statusCode = 503;
        this.rsp.setHeader("Retry-After", "60");
        throw new YtError("This proxy is banned.");
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

YtCommand.prototype._redirectHeavyRequests = function() {
    "use strict";
    this.__DBG("_redirectHeavyRequests");

    if (this.descriptor.is_heavy && this.coordinator.getSelf().role !== "data") {
        var target = this.coordinator.allocateDataProxy();
        if (typeof(target) !== "undefined") {
            var isSsl;
            isSsl = this.req.connection.getCipher && this.req.connection.getCipher();
            isSsl = !!isSsl;
            var url =
                (isSsl ? "https://" : "http://") +
                target.host +
                this.req.originalUrl;
            utils.redirectTo(this.rsp, url, 307);
            throw new YtError();
        } else {
            this.rsp.statusCode = 503;
            this.rsp.setHeader("Retry-After", "60");
            throw new YtError("There are no data proxies available.");
        }
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
            result = new binding.TNodeWrap(
                header.trim(),
                binding.ECompression_None,
                _PREDEFINED_JSON_FORMAT);
        } catch (err) {
            throw new YtError("Unable to parse X-YT-Input-Format header.", err);
        }
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        var default_mime = _MIME_DEFAULT[this.descriptor.input_type_as_integer];
        result = default_mime ? _MIME_TO_FORMAT[default_mime] : _PREDEFINED_YSON_FORMAT;
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
            throw new YtError(
                "Unsupported Content-Encoding " + JSON.stringify(header) + "." +
                " Candidates are: " + _ENCODING_ALL.join(", ") + ".");
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
    var filename = undefined;
    var disposition = "attachment";

    // First, resolve content disposition.
    if (this.descriptor.is_heavy) {
        // Do our best to guess filename.
        var passed_path = this.req.parsedQuery["path"];
        if (typeof(passed_path) !== "undefined") {
            filename = "yt_" + passed_path;
        }
        var passed_filename = this.req.parsedQuery["filename"];
        if (typeof(passed_filename) !== "undefined") {
            filename = passed_filename;
        }

        // Sanitize filename.
        if (typeof(filename) !== "undefined") {
            filename = filename
                .replace(/[^a-zA-Z0-9-.]/g, '_')
                .replace(/_+/, '_')
                .replace(/^_/, '')
                .replace(/_$/, '');
        }

        // Do our best to guess disposition.
        // XXX(sandello): For STDERRs -- use inline disposition.
        if (typeof(filename) !== "undefined") {
            if (filename.match(/sys_operations_.*_stderr$/)) {
                disposition = "inline";
            }
        }
        var passed_disposition = this.req.parsedQuery["disposition"];
        if (typeof(passed_disposition) !== "undefined") {
            disposition = passed_disposition.toLowerCase();
        }

        // Sanitize disposition.
        if (disposition !== "attachment" && disposition !== "inline") {
            disposition = "attachment";
        }

        // Construct header.
        var resulting_header = disposition;
        if (typeof(filename) !== "undefined") {
            resulting_header += "; filename=\"" + filename + "\"";
        }

        this.rsp.setHeader("Content-Disposition", resulting_header);
    }

    // Now, check whether the command either produces no data or an octet stream.
    if (this.descriptor.output_type_as_integer === binding.EDataType_Null) {
        this.output_format = _PREDEFINED_YSON_FORMAT;
        this.mime_type = undefined;
        return;
    }

    if (this.descriptor.output_type_as_integer === binding.EDataType_Binary) {
        this.output_format = _PREDEFINED_YSON_FORMAT;
        // XXX(sandello): Allow browsers to display data inline.
        if (disposition === "inline") {
            this.mime_type = "text/plain";
        } else {
            this.mime_type = "application/octet-stream";
        }
        return;
    }

    // Now, try to deduce output format from Accept header.
    header = this.req.headers["accept"];
    if (typeof(header) === "string") {
        result_mime = utils.bestAcceptedType(
            _MIME_BY_OUTPUT_TYPE[this.descriptor.output_type_as_integer],
            header);

        if (!result_mime) {
            this.rsp.statusCode = 406;
            throw new YtError(
                "Could not determine feasible Content-Type given Accept constraints." +
                " Candidates are: " + _MIME_BY_OUTPUT_TYPE[this.descriptor.output_type_as_integer].join(", ") + ".");
        }

        result_format = _MIME_TO_FORMAT[result_mime];
    }

    // Now, try to deduce output format from our custom header.
    header = this.req.headers["x-yt-output-format"];
    if (typeof(header) === "string") {
        try {
            result_mime = "application/octet-stream";
            result_format = new binding.TNodeWrap(
                header.trim(),
                binding.ECompression_None,
                _PREDEFINED_JSON_FORMAT);
        } catch (err) {
            throw new YtError("Unable to parse X-YT-Output-Format header.", err);
        }
    }

    // Lastly, provide a default option.
    if (typeof(result_format) === "undefined") {
        result_mime = _MIME_DEFAULT[this.descriptor.output_type_as_integer];
        result_format = _MIME_TO_FORMAT[result_mime];
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

        // XXX(sandello): This is not implemented yet.
        if (result_mime === "x-lzop") { result_mime = undefined; }

        if (!result_mime) {
            this.rsp.statusCode = 415;
            throw new YtError(
                "Could not determine feasible Content-Encoding given Accept-Encoding constraints." +
                " Candidates are: " + _ENCODING_ALL.join(", ") + ".");
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
    var from_formats, from_url, from_header, from_body;

    try {
        from_formats = {
            input_format: this.input_format,
            output_format: this.output_format
        };
        from_formats = binding.CreateV8Node(from_formats);
    } catch (err) {
        throw new YtError("Unable to parse formats.", err);
    }

    try {
        from_url = utils.numerify(qs.parse(this.req.parsedUrl.query));

        // XXX(sandello): Once user overrided format specification,
        // we cannot do really much to infer MIME.
        if (typeof(from_url.output_format) !== "undefined") {
            this.mime_type = "application/octet-stream";
        }

        from_url = binding.CreateV8Node(from_url);
    } catch (err) {
        throw new YtError("Unable to parse parameters from the query string.", err);
    }

    try {
        header = this.req.headers["x-yt-parameters"];
        if (typeof(header) === "string") {
            from_header = new binding.TNodeWrap(
                header,
                binding.ECompression_None,
                _PREDEFINED_JSON_FORMAT);
        }
    } catch (err) {
        throw new YtError("Unable to parse parameters from the request header X-YT-Parameters.", err);
    }

    if (this.req.method === "POST") {
        // Here we heavily rely on fact that HTTP method was checked beforehand.
        // Moreover, there is a convention that mutating commands with structured input
        // are served with POST method (see |_checkHttpMethod|).
        from_body = this._captureBody();
        this.input_stream = new utils.NullStream();
        this.output_stream = this.rsp;
        this.pause = utils.Pause(this.input_stream);
    } else {
        from_body = Q.resolve();
        this.input_stream = this.req;
        this.output_stream = this.rsp;
    }

    var self = this;

    return Q
        .all([from_formats, from_url, from_header, from_body])
        .spread(function() {
            self.parameters = binding.CreateMergedNode.apply(
                undefined,
                arguments);
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
                deferred.resolve(new binding.TNodeWrap(
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
        name               : this.name,
        user               : this.user,
        parameters         : this.parameters.Print(),
        input_format       : this.input_format.Print(),
        input_compression  : binding.ECompression[this.input_compression],
        output_format      : this.output_format.Print(),
        output_compression : binding.ECompression[this.output_compression]
    });
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

    if (!this.omit_trailers) {
        this.rsp.setHeader("Trailer", "X-YT-Error, X-YT-Response-Code, X-YT-Response-Message");
    }
};

YtCommand.prototype._execute = function(cb) {
    "use strict";
    this.__DBG("_execute");

    var self = this;

    return this.driver.execute(this.name, this.user,
        this.input_stream, this.input_compression,
        this.output_stream, this.output_compression,
        this.parameters, this.request_id,
        this.pause,
        function(key, value) {
            self.logger.debug(
                "Got a response parameter",
                { key: key, value: value.Print() });

            self.response_parameters.SetByYPath(
                "/" + utils.escapeYPath(key),
                value);

            // If headers are not sent yet, then update the header value.
            if (!self.rsp._header) {
                self.rsp.setHeader(
                    "X-YT-Response-Parameters",
                    self.response_parameters.Print(
                        binding.ECompression_None,
                        _PREDEFINED_JSON_FORMAT));
            }
        })
    .spread(
        function(result) {
            self.logger.debug(
                "Command '" + self.name + "' successfully executed",
                { result: result });
            return arguments;
        },
        function(result) {
            self.logger.debug(
                "Command '" + self.name + "' has failed to execute",
                { result: result });
            return arguments;
        })
    .spread(function(result, bytes_in, bytes_out) {
        self.bytes_in  = bytes_in;
        self.bytes_out = bytes_out;

        if (result.code === 0) {
            self.rsp.statusCode = 200;
        } else if (result.code < 0) {
            self.rsp.statusCode = 500;
        } else if (result.code > 0) {
            self.rsp.statusCode = 400;
        }

        return result;
    });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtCommand;
