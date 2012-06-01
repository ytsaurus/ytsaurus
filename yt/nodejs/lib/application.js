var url = require("url");
var crypto = require("crypto");
var querystring = require("querystring");

var utils = require("./utils");
var ytnode_wrappers = require("./ytnode_wrappers");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { console.error("YT HTTP:", x); };
} else {
    __DBG = function( ) { };
}

// This mapping defines how MIME types map onto YT format specifications.
var _MIME_FORMAT_MAPPING = {
    "application/json" : "json",
    "yandex/yt-yson-binary" : "<format=binary;enable_raw=true>yson",
    "yandex/yt-yson-text" : "<format=text;enable_raw=false>yson",
    "yandex/yt-yson-pretty": "<format=pretty;enable_raw=false>yson",
    "text/csv" : "csv",
    "text/tab-separated-values" : "tsv"
};

// This mapping defines which HTTP methods various YT data types require.
var _DATA_TYPE_TO_METHOD_MAPPING = {
    "Null" : "GET",
    "Binary" : "PUT",
    "Structured" : "POST",
    "Tabular" : "PUT"
};

////////////////////////////////////////////////////////////////////////////////
// RSP Utilities

function _rspSendError(rsp, code, text) {
    // TODO(sandello): Maybe answer with other Content -Type.
    var body = JSON.stringify({ error : text + "\nPlease consult API reference for further information." });
    rsp.writeHead(code, {
        "Content-Length" : body.length,
        "Content-Type"   : "application/json"
    });
    rsp.end(body);
}

function _rspSetFormatHeaders(rsp, input_format, output_format) {
    rsp.setHeader("X-YT-Input-Format", input_format);
    rsp.setHeader("X-YT-Output-Format", output_format);

    // TODO(sandello): Strip off all atributes before checking.
    for (var mime in _MIME_FORMAT_MAPPING) {
        if (output_format === _MIME_FORMAT_MAPPING[mime]) {
            rsp.setHeader("Content-Type", mime);
        }
    }
}

function _rspSetHeaders(rsp) {
    rsp.setHeader("Transfer-Encoding", "chunked");
    rsp.setHeader("Trailer", "X-YT-Response");
}

////////////////////////////////////////////////////////////////////////////////
// REQ Utilities

function _reqHash(req) {
    var hash = crypto.createHash("sha1");
    hash.update(JSON.stringify(req.method));
    hash.update(JSON.stringify(req.url));
    hash.update(JSON.stringify(req.headers));
    hash.update(JSON.stringify(req.trailers));
    hash.update(JSON.stringify(req.httpVersion));
    return hash.digest("base64");
}

function _reqExtractName(req) {
    var name = req.parsedUrl.pathname.slice(1).toLowerCase();
    if (!/^[a-z_]+$/.test(name)) {
        return new Error("Malformed command '" + name + "'.");
    } else {
        return name;
    }
}

function _reqExtractParameters(req) {
    var parameters = querystring.parse(req.parsedUrl.query);
    if (!parameters) {
        return new Error("Unable to parse parameters from the query string.");
    } else {
        return parameters;
    }
}

function _reqExtractInputFormat(req) {
    var result, format, header;

    // Firstly, try to deduce input format from Content-Type header.
    header = req.headers["content-type"];
    if (typeof(header) === "string") {
        for (var mime in _MIME_FORMAT_MAPPING) {
            if (utils.is(mime, header)) {
                result = _MIME_FORMAT_MAPPING[mime];
                break;
            }
        }
    }

    // Secondly, try to deduce output format from our custom header.
    header = req.headers["x-yt-input-format"];
    if (typeof(header) === "string") {
        result = header;
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        result = "yson";
    }

    return result;
}

function _reqExtractOutputFormat(req) {
    var result, format, header;

    // Firstly, try to deduce output format from Accept header.
    header = req.headers["accept"];
    if (typeof(header) === "string") {
        for (var mime in _MIME_FORMAT_MAPPING) {
            if (mime === utils.accepts(mime, header)) {
                result = _MIME_FORMAT_MAPPING[mime];
                break;
            }
        }
    }

    // Secondly, try to deduce output format from our custom header.
    header = req.headers["x-yt-output-format"];
    if (typeof(header) === "string") {
        result = header;
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        result = "<format=text;enable_raw=false>yson";
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

function _dispatch(driver, req, rsp) {
    req.parsedUrl = url.parse(req.url);

    var hash = _reqHash(req);
    var name = _reqExtractName(req);
    var parameters = _reqExtractParameters(req);
    var input_format = _reqExtractInputFormat(req);
    var output_format = _reqExtractOutputFormat(req);

    if (name instanceof Error) {
        return _rspSendError(rsp, 404, name.message);
    }

    if (parameters instanceof Error) {
        return _rspSendError(rsp, 400, parameters.message);
    }

    if (input_format instanceof Error) {
        return _rspSendError(rsp, 415, input_format.message);
    }

    if (output_format instanceof Error) {
        return _rspSendError(rsp, 415, output_format.message);
    }

    var descriptor = driver.find_command_descriptor(name);

    __DBG("Cmd hash=" + hash);
    __DBG("Cmd name=" + name);
    __DBG("Cmd descriptor=" + JSON.stringify(descriptor));
    __DBG("Cmd parameters=" + JSON.stringify(parameters));
    __DBG("Cmd input_format=" + input_format);
    __DBG("Cmd output_format=" + output_format);

    if (descriptor === null) {
        return _rspSendError(rsp, 404,
            "There is no such command '" + name + "' registered.");
    }

    var input_type_as_string = ytnode_wrappers.EDataType[descriptor.input_type];
    var output_type_as_string = ytnode_wrappers.EDataType[descriptor.output_type];
    var expected_http_method = _DATA_TYPE_TO_METHOD_MAPPING[input_type_as_string];

    __DBG("Cmd input_type_as_string=" + input_type_as_string);
    __DBG("Cmd output_type_as_string=" + output_type_as_string);
    __DBG("Cmd expected_http_method=" + expected_http_method);

    if (req.method != expected_http_method) {
        rsp.setHeader("Allow", expected_http_method);
        return _rspSendError(rsp, 405,
            "Command '" + name + "' expects " + input_type_as_string.toLowerCase() + " input and hence have to be requested with the " + expected_http_method + " method.");
    }

    _rspSetFormatHeaders(rsp, input_format, output_format);
    _rspSetHeaders(rsp);

    rsp.writeHead(200);

    // TODO(sandello): Handle various return-types here.
    driver.execute(name,
        req, input_format,
        rsp, output_format,
        parameters, function(code, message) {
            __DBG("Cmd hash=" + hash + " -> done");
            rsp.addTrailers({ "X-YT-Response" : code + " " + message });
        });
}

////////////////////////////////////////////////////////////////////////////////

function YtApplication(configuration) {
    var driver = new ytnode_wrappers.YtDriver(configuration);
    return function(req, rsp) {
        return _dispatch(driver, req, rsp);
    };
}

////////////////////////////////////////////////////////////////////////////////

exports.YtApplication = YtApplication;

