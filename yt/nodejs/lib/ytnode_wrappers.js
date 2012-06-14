var util = require("util");
var stream = require("stream");
var assert = require("assert");

var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __EOF = {};
var __DBG;

if (process.env.NODE_DEBUG && /YTNODE/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { console.error("YT Wrappers:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function( ) { };
}

////////////////////////////////////////////////////////////////////////////////

function YtReadableStream() {
    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG("Readable (" + this.__UUID + ") -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function( ) { };
    }

    this.__DBG("New");
    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    this._pending = [];
    this._paused = false;
    this._ended = false;

    var self = this;

    this._binding = new binding.TNodeJSOutputStream();
    this._binding.on_write = function(chunk) {
        self.__DBG("Bindings -> on_write");
        if (!self.readable || self._ended) {
            return;
        }
        if (self._paused || self._pending.length) {
            self._pending.push(chunk);
        } else {
            assert.ok(Buffer.isBuffer(chunk));
            self._emitData(chunk);
        }
    };
    this._binding.on_drain = function() {
        self.__DBG("Bindings -> on_drain");
        self.emit("_drain");
    };
    this._binding.on_flush = function() {
        self.__DBG("Bindings -> on_flush");
        self.emit("_flush");
    };
    this._binding.on_finish = function() {
        self.__DBG("Bindings -> on_finish");
        self._endSoon();
    };
}

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._emitData = function(chunk) {
    this.__DBG("_emitData");
    this.emit("data", chunk);
};

YtReadableStream.prototype._emitEnd = function() {
    this.__DBG("_emitEnd");

    if (!this._ended) { 
        this.emit("end");
    }

    this.readable = false;
    this._ended = true;
};

YtReadableStream.prototype._emitQueue = function() {
    this.__DBG("_emitQueue");

    if (this._pending.length) {
        var self = this;
        process.nextTick(function() {
            self.__DBG("_emitQueue -> (inner-cycle)");
            while (self.readable && !self._ended && !self._paused && self._pending.length) {
                var chunk = self._pending.shift();
                if (chunk !== __EOF) {
                    assert.ok(Buffer.isBuffer(chunk));
                    self._emitData(chunk);
                } else {
                    assert.ok(self._pending.length === 0);
                    self._emitEnd(chunk);
                }
            }
        });
    }
};

YtReadableStream.prototype._endSoon = function() {
    this.__DBG("_endSoon");

    if (!this.readable || this._ended) {
        return;
    }

    if (this._binding.IsEmpty()) {
        var self = this;
        process.nextTick(function() {
            self.__DBG("_endSoon -> (inner-tick)");
            if (self._paused || self._pending.length) {
                self._pending.push(__EOF);
            } else {
                assert.ok(self._pending.length === 0);
                self._emitEnd();
            }
        });
    } else {
        this.once("_drain", this._endSoon.bind(this));
    }
};

YtReadableStream.prototype.pause = function() {
    this.__DBG("pause");
    this._paused = true;
};

YtReadableStream.prototype.resume = function() {
    this.__DBG("resume");
    this._paused = false;
    this._emitQueue();
};

YtReadableStream.prototype.destroy = function() {
    this.__DBG("destroy");
    this._binding.Destroy();
    this.readable = false;
    this._ended = true;
};

////////////////////////////////////////////////////////////////////////////////

function YtWritableStream() {
    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG("Writable (" + this.__UUID + ") -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function( ) { };
    }

    this.__DBG("New");
    stream.Stream.call(this);

    this.readable = false;
    this.writable = true;

    this._ended = false;
    this._closed = false;

    this._binding = new binding.TNodeJSInputStream();
}

util.inherits(YtWritableStream, stream.Stream);

YtWritableStream.prototype._emitClose = function() {
    this.__DBG("_emitClose");

    if (!this._closed) {
        this.emit("close");
    }

    this._closed = true;
};

YtWritableStream.prototype.write = function(chunk, encoding) {
    this.__DBG("write");

    if (typeof(chunk) !== "string" && !Buffer.isBuffer(chunk)) {
        throw new TypeError("Expected first argument to be a String or Buffer");
    }

    if (typeof(chunk) === "string") {
        chunk = new Buffer(chunk, encoding);
    }

    if (!this._ended) {
        this._binding.Push(chunk, 0, chunk.length);
        return true;
    } else {
        return false;
    }
};

YtWritableStream.prototype.end = function(chunk, encoding) {
    this.__DBG("end");

    if (chunk) {
        this.write(chunk, encoding);
    }

    this._binding.End();

    this.writable = false;
    this._ended = true;

    var self = this;
    process.nextTick(function() { self._emitClose(); });
};

YtWritableStream.prototype.destroy = function() {
    this.__DBG("destroy");

    this._binding.Destroy();

    this.writable = false;
    this._ended = true;
    this._closed = true;
};

////////////////////////////////////////////////////////////////////////////////

function YtDriver(configuration) {
    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG("Driver (" + this.__UUID + ") -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function( ) { };
    }

    this.__DBG("New");

    this._binding = new binding.TNodeJSDriver(configuration);
}

YtDriver.prototype.execute = function(name,
    input_stream, input_format,
    output_stream, output_format,
    parameters, cb
) {
    this.__DBG("execute");

    var wrapped_input_stream = new YtWritableStream();
    var wrapped_output_stream = new YtReadableStream();

    this.__DBG("execute <<(" + wrapped_input_stream.__UUID + ") >>(" + wrapped_output_stream.__UUID + ")");

    input_stream.pipe(wrapped_input_stream);
    wrapped_output_stream.pipe(output_stream);

    var self = this;

    function on_error(err) {
        self.__DBG("execute -> (on-error callback)");
        wrapped_input_stream.destroy();
        wrapped_output_stream.destroy();
    }

    input_stream.on("error", on_error);
    output_stream.on("error", on_error);

    var result = this._binding.Execute(name,
        wrapped_input_stream._binding, input_format,
        wrapped_output_stream._binding, output_format,
        parameters, function()
    {
        self.__DBG("execute -> (on-execute callback)");
        cb.apply(this, arguments);
        wrapped_output_stream._endSoon();
    });
};

YtDriver.prototype.find_command_descriptor = function(command_name) {
    this.__DBG("find_command_descriptor");
    return this._binding.FindCommandDescriptor(command_name);
};

YtDriver.prototype.get_command_descriptors = function() {
    this.__DBG("get_command_descriptors");
    return this._binding.GetCommandDescriptors();
};

////////////////////////////////////////////////////////////////////////////////

exports.YtReadableStream = YtReadableStream;
exports.YtWritableStream = YtWritableStream;

exports.YtDriver = YtDriver;

exports.EDataType = binding.EDataType;

exports.GetEioStatistics = binding.GetEioStatistics;
