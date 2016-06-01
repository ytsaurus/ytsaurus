var util = require("util");
var stream = require("stream");
var assert = require("assert");

var binding = require("./ytnode");

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

    if (this._paused || this._ended || this._closed) {
        return;
    }

    if (!this._binding.IsFlowing()) {
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
            this.emit("data", chunk);
        }
    }

    if (i > 0) {
        process.nextTick(this._flow.bind(this));
    } else {
        if (this._binding.IsFinished()) {
            this._emitEnd();
        }
    }
};

YtReadableStream.prototype._emitEnd = function YtReadableStream$_emitEnd()
{
    "use strict";
    this.__DBG("_emitEnd");

    if (!this._ended) {
        this._ended = true;
        this.emit("end");
    }

    this._emitClose();
};

YtReadableStream.prototype._emitClose = function YtReadableStream$_emitClose()
{
    "use strict";
    this.__DBG("_emitClose");

    if (this._closed) {
        this._closed = true;

        this.readable = false;
        this._binding = null;

        this.emit("close");
    }
}

YtReadableStream.prototype.pause = function YtReadableStream$pause()
{
    "use strict";
    this.__DBG("pause");

    this._paused = true;
};

YtReadableStream.prototype.resume = function YtReadableStream$resume()
{
    "use strict";
    this.__DBG("resume");

    if (this._paused) {
        this._paused = false;
        process.nextTick(this._flow.bind(this));
    }
};

YtReadableStream.prototype.destroy = function YtReadableStream$destroy()
{
    "use strict";
    this.__DBG("destroy");

    if (this._binding) {
        this._binding.Destroy();
    }

    this._paused = false;
    this._ended = true;

    this._emitClose();
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtReadableStream;
