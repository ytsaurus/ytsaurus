var Q = require("q");

var YtError = require("./error").that;
var YtReadableStream = require("./readable_stream").that;
var YtWritableStream = require("./writable_stream").that;

var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT(ALL|NODE)/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Driver:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

function promisinglyPipe(source, destination)
{
    "use strict";

    var deferred = Q.defer();

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

    function on_end() {
        deferred.resolve();
    }
    function on_source_close() {
        deferred.reject(new YtError("Source stream in the pipe has been closed."));
    }
    function on_destination_close() {
        deferred.reject(new YtError("Destination stream in the pipe has been closed."));
    }
    function on_error(err) {
        cleanup();
        deferred.reject(err);
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

    return deferred.promise;
}

////////////////////////////////////////////////////////////////////////////////

function YtDriver(echo, config) {
    "use strict";

    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG(this.__UUID + " -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function(){};
    }

    this.__DBG("New");

    this.low_watermark = config.low_watermark;
    this.high_watermark = config.high_watermark;

    this.__DBG("low_watermark = " + this.low_watermark);
    this.__DBG("high_watermark = " + this.high_watermark);

    this._binding = new binding.TNodeJSDriver(echo, config.proxy);
}

YtDriver.prototype.execute = function(name,
    input_stream, input_compression, input_format,
    output_stream, output_compression, output_format,
    parameters
) {
    "use strict";
    this.__DBG("execute");

    var wrapped_input_stream = new YtWritableStream(this.low_watermark, this.high_watermark);
    var wrapped_output_stream = new YtReadableStream(this.low_watermark, this.high_watermark);

    this.__DBG("execute <<(" + wrapped_input_stream.__UUID + ") >>(" + wrapped_output_stream.__UUID + ")");

    var deferred = Q.defer();
    var self = this;

    var input_pipe_promise = Q.when(
        promisinglyPipe(input_stream, wrapped_input_stream),
        function() { wrapped_input_stream.end(); },
        function(err) {
            input_stream.destroy();
            wrapped_input_stream.destroy();
            return YtError.ensureWrapped(err);
        });

    var output_pipe_promise = Q.when(
        promisinglyPipe(wrapped_output_stream, output_stream),
        function() { }, // Do not close |output_stream| here since we have to write out trailers.
        function(err) {
            output_stream.destroy();
            wrapped_output_stream.destroy();
            return YtError.ensureWrapped(err);
        });

    this._binding.Execute(name,
        wrapped_input_stream._binding, input_compression, input_format,
        wrapped_output_stream._binding, output_compression, output_format,
        parameters, function(err)
    {
        self.__DBG("execute -> (on-execute callback)");
        // XXX(sandello): Can we move |_endSoon| to C++?
        wrapped_output_stream._endSoon();
        if (err) {
            deferred.reject(new YtError("Unable to execute driver command", err));
        } else {
            deferred.resolve(Array.prototype.slice.call(arguments, 1));
        }
    });

    // Probably, pipe promises could abort the pipeline, but whatever,
    // Execute() promise will be rejected upon pipe failure.
    return Q
        .all([ deferred.promise, input_pipe_promise, output_pipe_promise ])
        .spread(function(result, ir, or) {
            if (result instanceof YtError) {
                if (ir) { result.inner_errors.push(ir); }
                if (or) { result.inner_errors.push(or); }
            }
            return result;
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
