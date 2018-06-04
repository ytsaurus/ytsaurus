var url = require("url");

var buffertools = require("buffertools");
var qs = require("qs");

var Q = require("bluebird");

var utils = require("./utils");
var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./ytnode");

var YtError = require("./error").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("C", "Command");

// This mapping defines the supported API versions.
var _VERSION_TO_FACADE = {
    2 : require("./driver_facade_v2.js").that,
    3 : require("./driver_facade_v3.js").that,
    4 : require("./driver_facade_v3.js").that,
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

function formatToMime(format)
{
    var format_name = format.Get();
    switch (format_name) {
        case "schemaful_dsv":
            return "text/tab-separated-values";
        case "yamred_dsv":
            return "text/tab-separated-values";
        case "yamr": {
            var lenval = format.FindByYPath("/@lenval").Get();
            var has_subkey = format.FindByYPath("/@has_subkey").Get();
            if (lenval) {
                if (has_subkey) {
                    return "application/x-yamr-subkey-lenval";
                } else {
                    return "application/x-yamr-lenval"
                }
            } else {
                if (has_subkey) {
                    return "application/x-yamr-subkey-delimited";
                } else {
                    return "application/x-yamr-delimited";
                }
            }
        }
        case "dsv": {
            var record_separator = format.FindByYPath("/@record_separator").Get();
            var key_value_separator = format.FindByYPath("/@key_value_separator").Get();
            var line_prefix = format.FindByYPath("/@line_prefix").Get();
            if (record_separator == "," && key_value_separator == ":") {
                return "text/csv";
            } else if (line_prefix == "tskv") {
                return "text/x-tskv";
            } else {
                return "text/tab-separated-values";
            }
        }
        case "json":
            return "application/json";
        case "yson": {
            var yson_format = format.FindByYPath("/@format").Get();
            if (yson_format == "text") {
                return "application/x-yt-yson-text";
            } else if (yson_format == "pretty") {
                return "application/x-yt-yson-pretty";
            } else {
                return "application/x-yt-yson-binary";
            }
        }
    }
}

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
    "br"       : binding.ECompression_Brotli,
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

function YtCommand(logger, driver, coordinator, watcher, sticky_cache, pause) {
    "use strict";
    this.__DBG = __DBG.Tagged();

    this.logger = logger;
    this.driver = driver;
    this.coordinator = coordinator;
    this.watcher = watcher;
    this.sticky_cache = sticky_cache;
    this.pause = pause;

    // This is a total list of class fields; keep this up to date to improve V8
    // performance (JIT code reuse depends on so-called "hidden class"; see links
    // on V8 optimization for more details).

    this.req = null;
    this.rsp = null;

    this.omit_trailers = undefined;
    this.request_id = undefined;

    this.api_version = undefined;
    this.command = undefined;
    this.user = undefined;

    this.header_format = _PREDEFINED_JSON_FORMAT;

    this.parameters = undefined;
    this.response_parameters = binding.CreateV8Node({});

    this.descriptor = undefined;

    this.input_compression = undefined;
    this.input_format = undefined;
    this.input_stream = undefined;

    this.output_compression = undefined;
    this.output_format = undefined;
    this.output_stream = undefined;
    this.memory_output = undefined;

    this.mime_compression = undefined;
    this.mime_type = undefined;

    this.__DBG("New");
}

YtCommand.prototype.dispatch = function(req, rsp) {
    this.__DBG("dispatch");

    var self = this;

    self.req = req;
    self.rsp = rsp;

    if (typeof(self.req.tags) === "undefined") {
        self.req.tags = {};
    }

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
            self._getHeaderFormat();
            self._getInputFormat();
            self._getInputCompression();
            self._getOutputFormat();
            self._getOutputCompression();
        })
        .then(self._captureParameters.bind(self))
        .then(function() {
            self._setContentDispositionAndMime();
            self._logRequest();
            self._addHeaders();
        })
        .then(self._execute.bind(self))
        .catch(function(err) {
            if (!(err instanceof YtError)) {
                self.rsp.statusCode = 500;
            }
            var error = YtError.ensureWrapped(
                err,
                "Unhandled error in the command pipeline");
            return [error, 0, 0];
        })
        .spread(function(result, bytes_in, bytes_out) {
            self._epilogue(result, bytes_in, bytes_out);
        })
        .done();
};

YtCommand.prototype._epilogue = function(result, bytes_in, bytes_out) {
    this.__DBG("_epilogue");

    if (result.code === 0) {
        this.rsp.statusCode = 200;
    } else if (result.code !== 0 && this.rsp.statusCode === 200) {
        this.rsp.statusCode = 400;
    }

    var sent_headers = !!this.rsp._header;

    if (result.isUnavailable() || result.isAllTargetNodesFailed()) {
        this.rsp.statusCode = 503;
        if (!sent_headers) {
            this.rsp.setHeader("Retry-After", "60");
        }
    }

    if (result.isUserBanned()) {
        this.rsp.statusCode = 403;
    }

    if (result.isRequestQueueSizeLimitExceeded()) {
        this.rsp.statusCode = 429;
    }

    if (result.isUserBanned()) {
        this.sticky_cache.set(this.user, {
            code: this.rsp.statusCode,
            body: (new YtError("User '" + this.user + "' is banned")).toJson()
        });
    }

    var extra_headers = {
        "X-YT-Error": result.toJson(),
        "X-YT-Response-Code": utils.escapeHeader(result.getCode()),
        "X-YT-Response-Message": utils.escapeHeader(result.getMessage()),
        "X-YT-Response-Parameters": this.response_parameters.Print(binding.ECompression_None, this.header_format),
    };

    if (!sent_headers) {
        this.rsp.removeHeader("Trailer");
        for (var p in extra_headers) {
            if (extra_headers.hasOwnProperty(p)) {
                this.rsp.setHeader(p, extra_headers[p]);
            }
        }
    } else if (!this.omit_trailers) {
        this.rsp.addTrailers(extra_headers);
    }

    this.logger.debug("Done (" + (result.isOK() ? "success" : "failure") + ")", {
        bytes_in: bytes_in,
        bytes_out: bytes_out,
        result: result,
    });

    if (!result.isOK() && !sent_headers) {
        utils.dispatchAs(
            this.rsp,
            result.toJson(),
            "application/json");
    } else {
        if (this.memory_output) {
            var chunks = this.output_stream.chunks;
            var length = 0;
            var i, n;
            for (i = 0, n = chunks.length; i < n; ++i) {
                length += chunks[i].length;
            }
            this.rsp.removeHeader("Transfer-Encoding");
            this.rsp.setHeader("Content-Length", length);
            for (i = 0, n = chunks.length; i < n; ++i) {
                this.rsp.write(chunks[i]);
            }
        }
        this.rsp.end();
    }
};

YtCommand.prototype._parseRequest = function() {
    this.__DBG("_parseRequest");

    this.req.parsedUrl = url.parse(this.req.url);
    this.req.parsedQuery = qs.parse(this.req.parsedUrl.query);

    // Do not care about 'null' or 'undefined' in code below.
    var ua = this.req.headers["user-agent"] + "";
    var is_ie = ua.indexOf("Trident") !== -1;

    // XXX(sandello): IE is bugged; it fails to parse request with trailing
    // headers that include colons. Remarkable.
    this.omit_trailers = is_ie;

    var has_omit_trailers_option = typeof(this.req.headers["x-yt-omit-trailers"]) !== "undefined";

    if (has_omit_trailers_option) {
        this.omit_trailers = true;
    }
};

YtCommand.prototype._getName = function() {
    this.__DBG("_getName");

    var versioned_name = this.req.parsedUrl.pathname.slice(1).toLowerCase();

    var version;
    var name;
    var facade;
    var driver;

    if (!versioned_name.length) {
        utils.dispatchJson(this.rsp, Object.keys(_VERSION_TO_FACADE).map(function(version) {
            return "v" + version;
        }));
        throw new YtError();
    }

    var parts = versioned_name.split("/");
    version = parts[0];
    name = parts[1] || "";

    if (!/^v[0-9]+$/.test(version)) {
        throw new YtError("Malformed API version " + JSON.stringify(version));
    }

    version = parseInt(version.slice(1), 10);

    if (!_VERSION_TO_FACADE.hasOwnProperty(version)) {
        throw new YtError("Unsupported API version " + JSON.stringify(version));
    }

    facade = _VERSION_TO_FACADE[version];
    driver = facade(this.logger, this.driver);

    if (!name.length) {
        // Bail out to API description.
        utils.dispatchJson(
            this.rsp,
            driver.get_command_descriptors(version));
        throw new YtError();
    }

    if (!/^[a-z_]+$/.test(name)) {
        throw new YtError("Malformed command name " + JSON.stringify(name));
    }

    this.req.tags.api_version = version;
    this.req.tags.api_command = name;

    this.api_version = version;
    this.command = name;
    this.driver = driver;
};

YtCommand.prototype._getUser = function() {
    this.__DBG("_getUser");

    if (typeof(this.req.authenticated_user) === "string") {
        this.user = this.req.authenticated_user;
    } else {
        throw new YtError("Failed to identify user credentials");
    }

    this.req.tags.user = this.user;

    if (this.command === "ping_tx" || this.command === "parse_ypath") {
        // Do not check stickness for `ping_tx` command.
        return;
    }

    var sticky_result = this.sticky_cache.get(this.user);
    if (typeof(sticky_result) !== "undefined") {
        this.rsp.statusCode = sticky_result.code;
        utils.dispatchAs(this.rsp, sticky_result.body, "application/json");
        throw new YtError();
    }
};

YtCommand.prototype._getDescriptor = function() {
    this.__DBG("_getDescriptor");

    this.descriptor = this.driver.find_command_descriptor(this.api_version, this.command);

    if (!this.descriptor) {
        this.rsp.statusCode = 404;
        throw new YtError("Command '" + this.command + "' is not registered");
    }
};

YtCommand.prototype._checkHttpMethod = function() {
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
            "Command '" + this.command +
            "' have to be executed with the " +
            expected_http_method +
            " HTTP method while the actual one is " +
            actual_http_method);
    }
};

YtCommand.prototype._checkAvailability = function() {
    this.__DBG("_checkAvailability");

    if (this.coordinator.getSelf().banned) {
        this.rsp.statusCode = 503;
        this.rsp.setHeader("Retry-After", "60");
        throw new YtError(
            "This proxy is banned");
    }

    if (this.descriptor.is_heavy && this.watcher.isChoking()) {
        this.rsp.statusCode = 503;
        this.rsp.setHeader("Retry-After", "60");
        throw new YtError(
            "Command '" + this.command +
            "' is heavy and the proxy is currently under heavy load; " +
            "please try another proxy or try again later");
    }
};

YtCommand.prototype._redirectHeavyRequests = function() {
    this.__DBG("_redirectHeavyRequests");

    var is_heavy = this.descriptor.is_heavy;
    var is_control = this.coordinator.getSelf().role === "control";
    var is_suppress = typeof(this.req.headers["x-yt-suppress-redirect"]) !== "undefined";

    if (is_heavy && is_control && !is_suppress) {
        if (this.descriptor.input_type_as_integer !== binding.EDataType_Null) {
            this.rsp.statusCode = 503;
            this.rsp.setHeader("Retry-After", "60");
            throw new YtError(
                "Control proxy may not serve heavy requests with input data");
        }
        var target = this.coordinator.allocateProxy("data");
        if (target) {
            var proto = "http://";
            if (this.req.connection.getCipher && this.req.connection.getCipher()) {
                proto = "https://";
            }
            var target_host = target.host;
            var source_host = this.req.headers.host;
            if (typeof(source_host) === "string") {
                if (/yandex-team\.ru$/.test(source_host)) {
                    target_host = target_host.replace("yandex.net", "yandex-team.ru");
                }
            }
            var url = proto + target_host + this.req.originalUrl;
            utils.redirectTo(this.rsp, url, 307);
            throw new YtError();
        } else {
            this.rsp.statusCode = 503;
            this.rsp.setHeader("Retry-After", "60");
            throw new YtError(
                "There are no data proxies available");
        }
    }
};

YtCommand.prototype._getHeaderFormat = function() {
    this.__DBG("_getHeaderFormat");

    try {
        var header = this.req.headers["x-yt-header-format"];
        if (typeof(header) === "string") {
            this.header_format = new binding.TNodeWrap(
                header.trim(),
                binding.ECompression_None,
                _PREDEFINED_YSON_FORMAT);
        }
    } catch (err) {
        throw new YtError("Unable to parse X-YT-Header-Format header", err);
    }
};

YtCommand.prototype._getInputFormat = function() {
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
    try {
        header = utils.gather(this.req.headers, "x-yt-input-format");
        if (header) {
            result = new binding.TNodeWrap(
                header,
                binding.ECompression_None,
                this.header_format);
        }
    } catch (err) {
        throw new YtError("Unable to parse X-YT-Input-Format header", err);
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        var default_mime = _MIME_DEFAULT[this.descriptor.input_type_as_integer];
        result = default_mime ? _MIME_TO_FORMAT[default_mime] : _PREDEFINED_YSON_FORMAT;
    }

    this.input_format = result;
};

YtCommand.prototype._getInputCompression = function() {
    this.__DBG("_getInputCompression");

    var result, header;

    header = this.req.headers["content-encoding"];
    if (typeof(header) === "string") {
        header = header.trim();
        if (_ENCODING_TO_COMPRESSION.hasOwnProperty(header)) {
            result = _ENCODING_TO_COMPRESSION[header];
        } else {
            throw new YtError(
                "Unsupported Content-Encoding " + JSON.stringify(header) + "; " +
                "candidates are: " + _ENCODING_ALL.join(", "));
        }
    }

    if (typeof(result) === "undefined") {
        result = binding.ECompression_None;
    }

    this.input_compression = result;
};

YtCommand.prototype._setContentDispositionAndMime = function() {
    this.__DBG("_setContentDispositionAndMime");

    var disposition = "attachment";

    // First, resolve content disposition.
    if (this.descriptor.is_heavy) {
        // Do our best to guess filename.
        var filename;
        var passed_path = this.req.parsedQuery.path;
        if (typeof(passed_path) !== "undefined") {
            filename = "yt_" + passed_path;
        }

        if (this.command == "get_job_stderr") {
            filename = "job_stderr_{}_{}".format(
                this.parameters.GetByYPath("/operation_id").Get(),
                this.parameters.GetByYPath("/job_id").Get());
        }

        if (this.command == "get_job_fail_context") {
            filename = "fail_context_{}_{}".format(
                this.parameters.GetByYPath("/operation_id").Get(),
                this.parameters.GetByYPath("/job_id").Get());
        }

        var passed_filename = this.req.parsedQuery.filename;
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
            if (filename.match(/sys_operations_.*_stderr$/) || this.command == "get_job_stderr") {
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
        this.mime_type = undefined;
        return;
    }

    if (this.descriptor.output_type_as_integer === binding.EDataType_Binary) {
        // XXX(sandello): Allow browsers to display data inline.
        if (disposition === "inline") {
            this.mime_type = "text/plain; charset=\"utf-8\"";
        } else {
            this.mime_type = "application/octet-stream";
        }
        return;
    }

    this.mime_type = formatToMime(this.parameters.GetByYPath("/output_format"));
};

YtCommand.prototype._getOutputFormat = function() {
    this.__DBG("_getOutputFormat");

    var result_format, result_mime, header;
    var output_type_as_integer = this.descriptor.output_type_as_integer;

    if (output_type_as_integer === binding.EDataType_Null ||
        output_type_as_integer === binding.EDataType_Binary)
    {
        this.output_format = _PREDEFINED_YSON_FORMAT;
        return;
    }

    result_mime = _MIME_DEFAULT[this.descriptor.output_type_as_integer];
    result_format = _MIME_TO_FORMAT[result_mime];

    header = this.req.headers["accept"];
    if (typeof(header) === "string") {
        result_mime = utils.bestAcceptedType(
            _MIME_BY_OUTPUT_TYPE[this.descriptor.output_type_as_integer],
            header);

        if (!result_mime) {
            this.rsp.statusCode = 406;
            throw new YtError(
                "Could not determine feasible Content-Type given Accept constraints; " +
                "candidates are: " + _MIME_BY_OUTPUT_TYPE[this.descriptor.output_type_as_integer].join(", "));
        }

        result_format = _MIME_TO_FORMAT[result_mime];
    }

    try {
        header = utils.gather(this.req.headers, "x-yt-output-format");
        if (header) {
            result_format = new binding.TNodeWrap(
                header,
                binding.ECompression_None,
                this.header_format);
        }
    } catch (err) {
        throw new YtError("Unable to parse X-YT-Output-Format header", err);
    }

    this.output_format = result_format;
};

YtCommand.prototype._getOutputCompression = function() {
    this.__DBG("_getOutputCompression");

    var result_compression, result_mime, header;

    header = this.req.headers["accept-encoding"];
    if (typeof(header) === "string" && this.descriptor.compression !== false) {
        result_mime = utils.bestAcceptedEncoding(
            _ENCODING_ALL,
            header);

        // XXX(sandello): This is not implemented yet.
        if (result_mime === "x-lzop") { result_mime = undefined; }

        if (!result_mime) {
            this.rsp.statusCode = 415;
            throw new YtError(
                "Could not determine feasible Content-Encoding given Accept-Encoding constraints; " +
                "candidates are: " + _ENCODING_ALL.join(", "));
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
        throw new YtError("Unable to parse formats", err);
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
        throw new YtError("Unable to parse parameters from the query string", err);
    }

    try {
        header = utils.gather(this.req.headers, "x-yt-parameters");
        if (header) {
            from_header = new binding.TNodeWrap(
                header,
                binding.ECompression_None,
                this.header_format);
        }
    } catch (err) {
        throw new YtError("Unable to parse parameters from the request header X-YT-Parameters", err);
    }

    if (this.req.method === "POST") {
        // Here we heavily rely on fact that HTTP method was checked beforehand.
        // Moreover, there is a convention that mutating commands with structured input
        // are served with POST method (see |_checkHttpMethod|).
        from_body = this._captureBody();
        this.input_stream = new utils.NullStream();
        this.pause = utils.Pause(this.input_stream);
    } else {
        from_body = Q.resolve();
        this.input_stream = this.req;
    }

    if (this.descriptor.output_type_as_integer === binding.EDataType_Null ||
        this.descriptor.output_type_as_integer === binding.EDataType_Structured) {
        this.output_stream = new utils.MemoryOutputStream();
        this.memory_output = true;
    } else {
        this.output_stream = this.rsp;
        this.memory_output = false;
    }

    var self = this;

    return Q
        .all([from_formats, from_url, from_header, from_body])
        .spread(function() {
            self.parameters = binding.CreateMergedNode.apply(
                undefined,
                arguments);
            if (self.parameters.GetNodeType() !== "map") {
                throw new YtError("Parameters must be a map");
            }
        });
};

YtCommand.prototype._captureBody = function() {
    this.__DBG("_captureBody");

    // Avoid capturing |this| to avoid back references.
    var input_compression = this.input_compression;
    var input_format = this.input_format;
    var req = this.req;
    var pause = this.pause;

    return new Q(function(resolve, reject) {
        var clean = false;
        var chunks = [];

        function resolve_and_clear(value) {
            if (!clean) {
                cleanup();
                clean = true;
            }
            resolve(value);
        }

        function reject_and_clear(err) {
            if (!clean) {
                cleanup();
                clean = true;
            }
            reject(err);
        }

        function capture_body_on_data(chunk) {
            chunks.push(chunk);
        }

        function capture_body_on_end() {
            try {
                var body = buffertools.concat.apply(undefined, chunks);
                if (body.length) {
                    resolve_and_clear(new binding.TNodeWrap(body, input_compression, input_format));
                } else {
                    resolve_and_clear();
                }
            } catch (err) {
                reject_and_clear(new YtError("Unable to parse parameters from the request body", err));
            }
        }

        function capture_body_on_close() {
            reject_and_clear(new YtError("Stream was closed"));
        }

        function capture_body_on_error(err) {
            reject_and_clear(new YtError("An error occurred", err));
        }

        req.on("data", capture_body_on_data);
        req.on("end", capture_body_on_end);
        req.on("close", capture_body_on_close);
        req.on("error", capture_body_on_error);

        function cleanup() {
            req.removeListener("data", capture_body_on_data);
            req.removeListener("end", capture_body_on_end);
            req.removeListener("close", capture_body_on_close);
            req.removeListener("error", capture_body_on_error);
        }

        pause.unpause();
    });
};

YtCommand.prototype._logRequest = function() {
    this.__DBG("_logRequest");

    this.logger.debug("Gathered request parameters", {
        command            : this.command,
        user               : this.user,
        parameters         : this.parameters.Print(),
        input_format       : this.input_format.Print(),
        input_compression  : binding.ECompression[this.input_compression],
        output_format      : this.output_format.Print(),
        output_compression : binding.ECompression[this.output_compression]
    });
};

YtCommand.prototype._addHeaders = function() {
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
    this.__DBG("_execute");

    var self = this;

    var sema_command = this.command;
    var sema_user = this.user;

    if (!self.watcher.acquireThread(sema_user, sema_command)) {
        self.rsp.statusCode = 503;
        self.rsp.setHeader("Retry-After", "60");
        return Q.reject(new YtError(
            "There are too many concurrent requests being served at the moment; " +
            "please try another proxy or try again later"));
    }

    self.logger.debug("Command '" + self.command + "' (API v" + self.api_version + ") is being executed");
    return this.driver.execute(
        this.api_version,
        this.command, this.user,
        this.input_stream, this.input_compression,
        this.output_stream, this.output_compression,
        this.parameters, this.request_id,
        this.pause,
        function execute$response_parameters_consumer(key, value) {
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
                        self.header_format));
            }

            if (typeof(key) === "string" && key === "revision") {
                var etag_string = value.Print();
                // Etag has ui64 type that cannot be represented as javascript number.
                // Therefore we dump it into yson and strip "u" if necessary.
                if (etag_string[etag_string.length - 1] == "u") {
                    etag_string = etag_string.substr(0, etag_string.length - 1);
                }
                self.rsp.setHeader("ETag", etag_string);

                header = utils.gather(self.req.headers, "if-none-match");
                if (header && typeof(header) === "string" && header === etag_string) {
                    self.rsp.statusCode = 304;
                }
            }
        },
        function execute$interceptor(result, bytes_in, bytes_out) {
            self.watcher.releaseThread(sema_user, sema_command);
            self.logger.debug(
                "Command '" + self.command + "' has finished executing",
                { result: result });
        })
    .spread(function() { return arguments; }, function() { return arguments; });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtCommand;
