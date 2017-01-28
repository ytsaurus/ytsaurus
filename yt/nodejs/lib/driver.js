var util = require("util");

var buffertools = require("buffertools");
var Q = require("bluebird");
var _ = require("underscore");

var YtError = require("./error").that;
var YtReadableStream = require("./readable_stream").that;
var YtWritableStream = require("./writable_stream").that;

var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./ytnode");
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "Driver");

var _SIMPLE_EXECUTE_USER = "root";
var _SIMPLE_EXECUTE_FORMAT = binding.CreateV8Node("json");

////////////////////////////////////////////////////////////////////////////////

function promisinglyPipe(source, destination, debug_)
{
    return new Q(function promisinglyPipe$impl(resolve, reject) {
        var debug = debug_ ? debug_ : __DBG.Tagged("pipe");
        var clean = false;

        function resolve_and_clear() {
            debug("pipe$resolve_and_clear");
            if (!clean) {
                cleanup();
                clean = true;
            }
            resolve();
        }

        function reject_and_clear(err) {
            debug("pipe$reject_and_clear");
            if (!clean) {
                cleanup();
                clean = true;
            }
            reject(err);
        }

        function on_data(chunk) {
            debug("pipe$on_data");
            if (destination.write(chunk) === false) {
                debug("pipe$on_data -> pause");
                source.pause();
            }
        }

        source.on("data", on_data);

        function on_drain() {
            debug("pipe$on_drain");
            source.resume();
        }

        destination.on("drain", on_drain);

        function on_end() {
            debug("pipe$on_end");
            resolve_and_clear();
        }

        function on_source_close() {
            debug("pipe$on_source_close");
            reject_and_clear(new YtError("Source stream in the pipe has been closed"));
        }

        function on_destination_close() {
            debug("pipe$on_destination_close");
            reject_and_clear(new YtError("Destination stream in the pipe has been closed"));
        }

        function on_error(err) {
            debug("pipe$on_error");
            reject_and_clear(err);
        }

        source.on("end", on_end);
        source.on("close", on_source_close);
        source.on("error", on_error);

        destination.on("close", on_destination_close);
        destination.on("error", on_error);

        function cleanup() {
            debug("cleanup");

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

        destination.emit("pipe", source);
    });
}

function parseJsonRows(rowData) {
    // Last row is in fact empty string
    var rows = rowData
        .split('\n')
        .slice(0, -1);

    rows = _.map(rows, JSON.parse);
    return rows;
}

////////////////////////////////////////////////////////////////////////////////

function YtDriver(config, echo)
{
    this.__DBG = __DBG.Tagged();

    this.low_watermark = config.low_watermark;
    this.high_watermark = config.high_watermark;

    this.__DBG("low_watermark = " + this.low_watermark);
    this.__DBG("high_watermark = " + this.high_watermark);

    this._binding = new binding.TDriverWrap(!!echo, config.proxy);

    this.__DBG("New");
}

YtDriver.prototype.execute = function YtDriver$execute(
    name, user,
    input_stream, input_compression,
    output_stream, output_compression,
    parameters, request_id, pause,
    response_parameters_consumer,
    result_interceptor)
{
    this.__DBG("execute");

    // Avoid capturing |this| to avoid back references.
    var wrapped_input_stream = new YtWritableStream(this.low_watermark, this.high_watermark);
    var wrapped_output_stream = new YtReadableStream(this.high_watermark);
    var binding = this._binding;
    var debug = this.__DBG;

    // Setup pipes.
    function destroyer() {
        debug("destroyer");
        wrapped_input_stream.destroy();
        wrapped_output_stream.destroy();
        input_stream.destroy();
        output_stream.destroy();
    }

    var input_pipe_promise = promisinglyPipe(input_stream, wrapped_input_stream, debug)
        .then(
        function ip_promise_then() {
            debug("execute -> input_pipe_promise has been resolved");
            // Close input stream here to allow driver command to terminate.
            wrapped_input_stream.end();
        },
        function ip_promise_catch(err) {
            debug("execute -> input_pipe_promise has been rejected");
            destroyer();
            return Q.reject(new YtError("Input pipe has been canceled", err));
        });

    var output_pipe_promise = promisinglyPipe(wrapped_output_stream, output_stream, debug)
        .then(
        function op_promise_then() {
            debug("execute -> output_pipe_promise has been resolved");
            // Do not close |output_stream| here since we have to write out trailers.
        },
        function op_promise_catch(err) {
            debug("execute -> output_pipe_promise has been rejected");
            destroyer();
            return Q.reject(new YtError("Output pipe has been canceled", err));
        });

    var driver_promise = new Q(function YtDriver$execute$impl(resolve, reject) {
        var future = binding.Execute(name, user,
            wrapped_input_stream._binding, input_compression,
            wrapped_output_stream._binding, output_compression,
            parameters, request_id,
            function(result) {
                debug("execute -> (on-execute callback)");
                if (typeof(result_interceptor) === "function") {
                    debug("execute -> (interceptor)");
                    try {
                        result_interceptor(result);
                    } catch (ex) {
                    }
                }
                if (result.code === 0) {
                    debug("execute -> execute_promise has been resolved");
                    resolve(Array.prototype.slice.call(arguments));
                } else {
                    debug("execute -> execute_promise has been rejected");
                    reject(result);
                }
            },
            response_parameters_consumer);

        input_pipe_promise.error(function() { future.Cancel(); });

        output_pipe_promise.error(function() { future.Cancel(); });
    });

    process.nextTick(function() { pause.unpause(); });

    return Q
        .all([driver_promise, input_pipe_promise, output_pipe_promise])
        .spread(function spread(result, ir, or) {
            return result;
        });
};

YtDriver.prototype.executeSimpleWithUser = function(name, user, parameters, data)
{
    this.__DBG("executeSimple");

    var descriptor = this.find_command_descriptor(name);

    if (descriptor.input_type === "tabular") {
        data = data && _.map(data, JSON.stringify).join("\n");
    } else {
        data = data && JSON.stringify(data);
    }

    var input_stream = new utils.MemoryInputStream(data);
    var output_stream = new utils.MemoryOutputStream();
    var pause = utils.Pause(input_stream);

    parameters.input_format = parameters.input_format || "json";
    parameters.output_format = parameters.output_format || "json";

    return this.execute(name, user,
        input_stream, binding.ECompression_None,
        output_stream, binding.ECompression_None,
        binding.CreateV8Node(parameters), null, pause, function(){})
    .then(function(result) {
        var body = buffertools.concat.apply(undefined, output_stream.chunks);
        if (descriptor.output_type === "tabular") {
            return parseJsonRows(body.toString());
        } else if (body.length) {
            return JSON.parse(body);
        }
    });
};

YtDriver.prototype.executeSimple = function(name, parameters, data)
{
    return this.executeSimpleWithUser(name, _SIMPLE_EXECUTE_USER, parameters, data);
};

YtDriver.prototype.find_command_descriptor = function(name)
{
    this.__DBG("find_command_descriptor");
    return this._binding.FindCommandDescriptor(name);
};

YtDriver.prototype.get_command_descriptors = function()
{
    this.__DBG("get_command_descriptors");
    return this._binding.GetCommandDescriptors();
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtDriver;
