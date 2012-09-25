var binding = require("./ytnode");
var debug = require("./debug");

var YtReadableStream = require("./readable_stream").that;
var YtWritableStream = require("./writable_stream").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YTNODE/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Driver:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

function properlyPipe(source, destination, onError)
{
    "use strict";

    function on_data(chunk) {
        if (destination.writable && destination.write(chunk) === false) {
            source.pause();
        }
    }

    source.on("data", on_data);

    function on_drain() {
        if (source.readable) {
            source.resume();
        }
    }

    destination.on("drain", on_drain);

    var source_has_died = false;
    var destination_has_died = false;

    function on_end() {
        if (source_has_died) { return; }
        source_has_died = true;
        destination.end();
    }

    function on_source_close() {
        if (source_has_died) { return; }
        source_has_died = true;
        destination.destroy();
    }

    function on_destination_close() {
        if (destination_has_died) { return; }
        destination_has_died = true;
        source.destroy();
    }

    function on_error() {
        cleanup();
        if (!source_has_died) {
            source_has_died = true;
            destination.destroy();
        }
        if (!destination_has_died) {
            destination_has_died = true;
            source.destroy();
        }
    }

    source.on("end", on_end);
    source.on("close", on_source_close);
    source.on("error", on_error);

    destination.on("close", on_destination_close);
    destination.on("error", on_error);

    function cleanup() {
        source.removeListener("data", on_data);
        destination.removeListener("drain", on_drain);

        source.removeListener("end", on_end);
        source.removeListener("close", on_source_close);
        source.removeListener("error", on_error);

        destination.removeListener("close", on_destination_close);
        destination.removeListener("error", on_error);

        source.removeListener("end", cleanup);
        source.removeListener("close", cleanup);

        destination.removeListener("end", cleanup);
        destination.removeListener("close", cleanup);
    }

    source.on("end", cleanup);
    source.on("close", cleanup);

    destination.on("end", cleanup);
    destination.on("close", cleanup);

    destination.emit("pipe", source);

    return destination;
}

////////////////////////////////////////////////////////////////////////////////

function YtDriver(echo, configuration) {
    "use strict";
    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG(this.__UUID + " -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function(){};
    }

    this.__DBG("New");

    this.low_watermark = configuration.low_watermark;
    this.high_watermark = configuration.high_watermark;

    this.__DBG("low_watermark = " + this.low_watermark);
    this.__DBG("high_watermark = " + this.high_watermark);

    this._binding = new binding.TNodeJSDriver(echo, configuration.proxy);
}

YtDriver.prototype.execute = function(name,
    input_stream, input_compression, input_format,
    output_stream, output_compression, output_format,
    parameters, cb
) {
    "use strict";
    this.__DBG("execute");

    var wrapped_input_stream = new YtWritableStream(this.low_watermark, this.high_watermark);
    var wrapped_output_stream = new YtReadableStream(this.low_watermark, this.high_watermark);

    this.__DBG("execute <<(" + wrapped_input_stream.__UUID + ") >>(" + wrapped_output_stream.__UUID + ")");

    var self = this;
    var self_finished = false;

    properlyPipe(input_stream, wrapped_input_stream);
    properlyPipe(wrapped_output_stream, output_stream);

    // debug.TraceReadableStream(input_stream, "source input (req)");
    // debug.TraceWritableStream(wrapped_input_stream, "wrapped input (writable; TNodeJSInputStream)");
    // debug.TraceReadableStream(wrapped_output_stream, "wrapped output (readable; TNodeJSOutputStream)");
    // debug.TraceWritableStream(output_stream, "source output (rsp)");
    // debug.TraceSocket(input_stream.connection, "underlying socket");

    function on_error(err) {
        self.__DBG("execute -> (on-error callback)");
        if (!self_finished) {
            cb.call(null, new Error("I/O error while executing driver command: " + err.toString()));
            self_finished = true;
        }
    }

    input_stream.on("error", on_error);
    output_stream.on("error", on_error);

    var result = this._binding.Execute(name,
        wrapped_input_stream._binding, input_compression, input_format,
        wrapped_output_stream._binding, output_compression, output_format,
        parameters, function()
    {
        self.__DBG("execute -> (on-execute callback)");
        if (!self_finished) {
            cb.apply(null, arguments);
            wrapped_output_stream._endSoon();
            self_finished = true;
        }
    });
};

YtDriver.prototype.find_command_descriptor = function(command_name) {
    "use strict";
    this.__DBG("find_command_descriptor");
    return this._binding.FindCommandDescriptor(command_name);
};

YtDriver.prototype.get_command_descriptors = function() {
    "use strict";
    this.__DBG("get_command_descriptors");
    return this._binding.GetCommandDescriptors();
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtDriver;
