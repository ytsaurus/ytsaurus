var util = require("util");
var stream = require("stream");

var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "Writable Stream");

////////////////////////////////////////////////////////////////////////////////

function YtWritableStream(low_watermark, high_watermark)
{
    "use strict";
    stream.Stream.call(this);

    this.readable = false;
    this.writable = true;

    this._ended = false;
    this._closed = false;

    this._binding = new binding.TInputStreamWrap(low_watermark, high_watermark);
    this._binding.on_drain = this._onDrain.bind(this);

    this.__DBG = __DBG.Tagged(this._binding.cxx_id);
    this.__DBG("New");
}

util.inherits(YtWritableStream, stream.Stream);

YtWritableStream.prototype._onDrain = function YtWritableStream$_onDrain()
{
    "use strict";
    this.__DBG("Bindings (InputStream) -> on_drain");

    if (!this._ended) {
        this.emit("drain");
    } else {
        process.nextTick(this._emitClose.bind(this));
    }
};

YtWritableStream.prototype._emitClose = function YtWritableStream$_emitClose()
{
    "use strict";
    this.__DBG("_emitClose");

    if (!this._closed) {
        this._closed = true;

        this.emit("close");

        this.writable = false;
    }
};

YtWritableStream.prototype.write = function YtWritableStream$write(chunk, encoding)
{
    "use strict";
    this.__DBG("write");

    if (this._ended || this._closed) {
        return false;
    }

    if (typeof(chunk) !== "string" && !Buffer.isBuffer(chunk)) {
        throw new TypeError("Expected first argument to be a String or a Buffer");
    }

    if (typeof(chunk) === "string") {
        chunk = new Buffer(chunk, encoding);
    }

    if (this._binding.Push(chunk, 0, chunk.length)) {
        return true;
    } else {
        this.__DBG("write -> (queue is full)");
        return false;
    }
};

YtWritableStream.prototype.end = function YtWritableStream$end(chunk, encoding)
{
    "use strict";
    this.__DBG("end");

    if (chunk) {
        this.write(chunk, encoding);
    }

    this.destroySoon();
};

YtWritableStream.prototype.destroySoon = function YtWritableStream$destroySoon()
{
    "use strict";
    this.__DBG("destroySoon");

    this._binding.End();

    this._ended = true;
};

YtWritableStream.prototype.destroy = function YtWritableStream$destroy()
{
    "use strict";
    this.__DBG("destroy");

    this._binding.Destroy();

    this._ended = true;

    this._emitClose();
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtWritableStream;
