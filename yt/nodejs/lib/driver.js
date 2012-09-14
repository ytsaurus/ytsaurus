var binding = require("./ytnode");

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
    var wrapped_output_stream = new YtReadableStream();

    this.__DBG("execute <<(" + wrapped_input_stream.__UUID + ") >>(" + wrapped_output_stream.__UUID + ")");

    input_stream.pipe(wrapped_input_stream);
    wrapped_output_stream.pipe(output_stream);

    var self = this;
    var self_finished = false;

    function on_error(err) {
        self.__DBG("execute -> (on-error callback)");
        wrapped_input_stream.destroy();
        wrapped_output_stream.destroy();
        if (!self_finished) {
            cb.call(self, new Error("I/O error while executing driver command"));
            self_finished = true;
        }
    }

    input_stream.on("error", on_error);
    output_stream.on("error", on_error);

    var result = this._binding.Execute(name,
        wrapped_input_stream._binding, input_compression, input_format,
        wrapped_output_stream._binding, output_compression, output_format,
        parameters, function(err, code, message, bytes_in, bytes_out)
    {
        self.__DBG("execute -> (on-execute callback)");
        if (!self_finished) {
            cb.call(this, err, code, message, bytes_in, bytes_out);
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
