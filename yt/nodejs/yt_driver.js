var util = require('util');
var stream = require('stream');
var assert = require('assert');

var yt_streams = require("./yt_streams")
var binding = require('./build/Release/yt_driver');

////////////////////////////////////////////////////////////////////////////////

var __EOF = {};
var __DBG;

if (process.env.NODE_DEBUG && /YT/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { console.error("YT:", x); };
} else {
    __DBG = function( ) { };
}

////////////////////////////////////////////////////////////////////////////////

function YtDriver() {
    __DBG("Driver -> New");

    this._binding = new binding.TNodeJSDriver();
}

YtDriver.prototype.execute = function(name,
    input_stream, input_format,
    output_stream, output_format,
    parameters, callback
) {
    __DBG("Driver -> Execute");

    if (!input_stream instanceof yt_streams.YtWritableStream) {
        throw new Error("input_stream have to be an instance of YtWritableStream");
    }

    if (!output_stream instanceof yt_streams.YtReadableStream) {
        throw new Error("output_stream have to be an instance of YtReadableStream");
    }

    this._binding.Execute(name,
        input_stream._binding, input_format,
        output_stream._binding, output_format,
        parameters, callback);
}

////////////////////////////////////////////////////////////////////////////////

exports.YtDriver = YtDriver;
