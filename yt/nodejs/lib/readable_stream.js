var util = require("util");
var stream = require("stream");
var assert = require("assert");

var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "Readable Stream");

////////////////////////////////////////////////////////////////////////////////

function YtReadableStream(watermark)
{
    "use strict";
    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    this._paused = false;
    this._ended = false;
    this._closed = false;

    this._binding = new binding.TOutputStreamWrap(watermark);
    this._binding.on_flowing = this._flow.bind(this);

    this.__DBG = __DBG.Tagged(this._binding.cxx_id);
    this.__DBG("New");
}

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._flow = function YtReadableStream$_flow()
{
    "use strict";
    this.__DBG("_flow");

    if (this._paused || this._ended) {
        return;
    }

    var i, n, chunk, result;
    result = this._binding.Pull();

    this.__DBG("Bindings (OutputStream) <- Pull");

    for (i = 0, n = result.length; i < n; ++i) {
        chunk = result[i];
        if (!chunk) {
            break;
        } else {
            this._emitData(chunk);
        }
    }

    if (i === 0 && this._binding.Drain()) {
        this._emitEnd();
    } else {
        process.nextTick(this._flow.bind(this));
    }
};

YtReadableStream.prototype._emitData = function YtReadableStream$_emitData(chunk)
{
    "use strict";
    this.__DBG("_emitData");

    if (!this._ended) {
        this.emit("data", chunk);
    }
};

YtReadableStream.prototype._emitEnd = function YtReadableStream$_emitEnd()
{
    "use strict";
    this.__DBG("_emitEnd");

    if (!this._ended) {
        this._ended = true;

        this.emit("end");

        this.readable = false;
    }

    process.nextTick(this._emitClose.bind(this));
};

YtReadableStream.prototype._emitClose = function YtReadableStream$_emitClose()
{
    "use strict";
    this.__DBG("_emitClose");

    if (this._closed) {
        this._closed = true;

        this.emit("close");

        this.readable = false;
    }
};

YtReadableStream.prototype.pause = function YtReadableStream$pause()
{
    "use strict";
    this.__DBG("pause");

    if (!this._ended) {
        this._paused = true;
    }
};

YtReadableStream.prototype.resume = function YtReadableStream$resume()
{
    "use strict";
    this.__DBG("resume");

    if (!this._ended && this._paused) {
        this._paused = false;
        process.nextTick(this._flow.bind(this));
    }
};

YtReadableStream.prototype.destroy = function YtReadableStream$destroy()
{
    "use strict";
    this.__DBG("destroy");

    this._binding.Destroy();

    this._paused = false;
    this._ended = true;

    this._emitClose();
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtReadableStream;
