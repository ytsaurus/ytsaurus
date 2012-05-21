var util = require('util');
var stream = require('stream');
var assert = require('assert');

var binding = require('ytnode');

////////////////////////////////////////////////////////////////////////////////

var __EOF = {};
var __DBG;

if (process.env.NODE_DEBUG && /YT/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { console.error("YT:", x); };
} else {
    __DBG = function( ) { };
}

////////////////////////////////////////////////////////////////////////////////

function YtReadableStream() {
    __DBG("Readable -> New");
    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    this._pending = [];
    this._paused = false;
    this._ended = false;

    var self = this;
    
    this._binding = new binding.TNodeJSOutputStream();
    this._binding.on_write = function(chunk) {
        __DBG("Readable -> Bindings -> on_write");
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
    this._binding.on_flush = function() {
        __DBG("Readable -> Bindings -> on_flush");
        self._emitQueue();
    };
    this._binding.on_finish = function() {
        __DBG("Readable -> Bindings -> on_finish");
        if (!self.readable || self._ended) {
            return;
        }
        if (self._paused || self._pending.length) {
            self._pending.push(__EOF);
        } else {
            assert.ok(self._pending.length === 0);
            self._emitEnd();
        }
    };
};

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._emitData = function(chunk) {
    __DBG("Readable -> _emitData");
    this.emit('data', chunk);
};

YtReadableStream.prototype._emitEnd = function() {
    __DBG("Readable -> _emitEnd");
    if (!this._ended) { 
        this.emit('end');
    }

    this.readable = false;
    this._ended = true;
}

YtReadableStream.prototype._emitQueue = function(callback) {
    __DBG("Readable -> _emitQueue");
    if (this._pending.length) {
        var self = this;
        process.nextTick(function() {
            __DBG("Readable -> _emitQueue -> (inner-cycle)");
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
            if (callback) {
                callback();
            }
        });
    } else {
        if (callback) {
            callback();
        }
    }
};

YtReadableStream.prototype.pause = function() {
    __DBG("Readable -> pause");
    this._paused = true;
};

YtReadableStream.prototype.resume = function() {
    __DBG("Readable -> resume");
    this._paused = false;
    this._emitQueue();
}

YtReadableStream.prototype.destroy = function() {
    __DBG("Readable -> destory");
    this.readable = false;
    this._ended = true;
}

////////////////////////////////////////////////////////////////////////////////

function YtWritableStream() {
    __DBG("Writable -> New")
    stream.Stream.call(this);

    this.readable = false;
    this.writable = true;

    this._ended = false;

    this._binding = new binding.TNodeJSInputStream();
};

util.inherits(YtWritableStream, stream.Stream);

YtWritableStream.prototype._emitClose = function() {
    __DBG("Writable -> _emitClose");
    if (!this._ended) {
        this.emit('close');
    }

    this.writable = false;
    this._ended = true;
}

YtWritableStream.prototype.write = function(chunk, encoding) {
    __DBG("Writable -> write");
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
}

YtWritableStream.prototype.end = function(chunk, encoding) {
    __DBG("Writable -> end");
    if (chunk) {
        this.write(chunk, encoding);
    }

    this._binding.Close();

    this.writable = false;
    this._ended = true;

    var self = this;
    process.nextTick(function() { self._emitClose(); });
}

YtWritableStream.prototype.destroy = function() {
    __DBG("Writable -> destroy");
    this._binding.Close();

    this.writable = false;
    this._ended = true;
}

////////////////////////////////////////////////////////////////////////////////

function YtDriver(configuration) {
    __DBG("Driver -> New");

    this._binding = new binding.TNodeJSDriver(configuration);
}

YtDriver.prototype.execute = function(name,
    input_stream, input_format,
    output_stream, output_format,
    parameters, callback
) {
    __DBG("Driver -> Execute");

    var wrapped_input_stream = new YtWritableStream();
    var wrapped_output_stream = new YtReadableStream();

    input_stream.pipe(wrapped_input_stream);
    wrapped_output_stream.pipe(output_stream);

    this._binding.Execute(name,
        wrapped_input_stream._binding, input_format,
        wrapped_output_stream._binding, output_format,
        parameters, callback);
}

////////////////////////////////////////////////////////////////////////////////

exports.YtReadableStream = YtReadableStream;
exports.YtWritableStream = YtWritableStream;

exports.YtDriver = YtDriver;
