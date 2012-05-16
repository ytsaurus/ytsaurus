var url = require("url");
var querystring = require("querystring");

var utils = require("./utils");
var yt_streams = require("./yt_streams");
var yt_driver = require("./yt_driver");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { console.error("YT HTTP:", x); };
} else {
    __DBG = function( ) { };
}

// This mapping defines how MIME types map onto YT format specifications.
var _FORMAT_MAPPING = {
    "application/json"          : "json",
    "text/csv"                  : "csv",
    "text/tab-separated-values" : "tsv"
};

////////////////////////////////////////////////////////////////////////////////
// RSP Utilities

function _rspSendError(rsp, code, text) {
    // TODO(sandello): Maybe answer with other Content -Type.
    var body = JSON.stringify({ error : text });
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
    for (var mime in _FORMAT_MAPPING) {
        if (output_format === _FORMAT_MAPPING[mime]) {
            rsp.setHeader("Content-Type", mime);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// REQ Utilities

function _reqExtractName(req) {
    var name = req.parsedUrl.pathname.slice(1).toLowerCase();
    if (!/^[a-z]+$/.test(name)) {
        return new Error("Malformed command name: '" + name + "'");
    } else {
        return name;
    }
}

function _reqExtractParameters(req) {
    var parameters = querystring.parse(req.parsedUrl.query);
    if (!parameters) {
        return new Error("Unable to parse parameters from query string");
    } else {
        return parameters;
    }
}

function _reqExtractInputFormat(req) {
    var result, format, header;

    // Firstly, try to deduce input format from Content-Type header.
    header = req.headers["content-type"];
    if (typeof(header) === "string") {
        for (var mime in _FORMAT_MAPPING) {
            if (utils.is(mime, header)) {
                result = _FORMAT_MAPPING[mime];
                break;
            }
        }
    }

    // Secondly, try to deduce output format from our header.
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
        for (var mime in _FORMAT_MAPPING) {
            if (mime === utils.accepts(mime, header)) {
                result = _FORMAT_MAPPING[mime];
                break;
            }
        }
    }

    // Secondly, try to deduce output format from our header.
    header = req.headers["x-yt-output-format"];
    if (typeof(header) === "string") {
        result = header;
    }

    // Lastly, provide a default option, i. e. YSON.
    if (typeof(result) === "undefined") {
        result = "yson";
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

function _dispatch(driver, req, rsp) {
    req.parsedUrl = url.parse(req.url);

    var name = _reqExtractName(req);
    var parameters = _reqExtractParameters(req);
    var input_format = _reqExtractInputFormat(req);
    var output_format = _reqExtractOutputFormat(req);

    if (name instanceof Error) {
        _rspSendError(rsp, 404, name.message);
    }

    if (parameters instanceof Error) {
        _rspSendError(rsp, 400, parameters.message);
    }

    if (input_format instanceof Error) {
        _rspSendError(rsp, 400, input_format.message);
    }

    if (output_format instanceof Error) {
        _rspSendError(rsp, 400, output_format.message);
    }

    __DBG("Cmd name=" + name);
    __DBG("Cmd parameters=" + JSON.stringify(parameters));
    __DBG("Cmd input_format=" + input_format);
    __DBG("Cmd output_format=" + output_format);

    _rspSetFormatHeaders(rsp, input_format, output_format);

    var input_stream = new yt_streams.YtWritableStream();
    var output_stream = new yt_streams.YtReadableStream();

    req.pipe(input_stream);
    output_stream.pipe(rsp);

    // TODO(sandello): Handle various return-types here.
    driver.execute(name,
        input_stream, input_format,
        output_stream, output_format,
        parameters, function() {
            rsp.end();
        });
}

////////////////////////////////////////////////////////////////////////////////

function YtApplication() {
    var driver = new yt_driver.YtDriver();
    return function(req, rsp) {
        return _dispatch(driver, req, rsp);
    };
}

////////////////////////////////////////////////////////////////////////////////

exports.YtApplication = YtApplication;
