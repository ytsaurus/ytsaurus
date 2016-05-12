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

    this._pending = [];

    this._binding = new binding.TOutputStreamWrap(watermark);
    this._binding.on_flowing = this._onFlowing.bind(this);

    this.__DBG = __DBG.Tagged(this._binding.cxx_id);
    this.__DBG("New");
}

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._onFlowing = function YtReadableStream$_onFlowing()
{
    "use strict";
    this.__DBG("Bindings (OutputStream) -> on_flowing");
    this._consumeData();
};

YtReadableStream.prototype._consumeData = function YtReadableStream$_consumeData()
{
    "use strict";
    this.__DBG("_consumeData");

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
            this.emit("data", chunk);
        }
    }

    if (i > 0) {
        process.nextTick(this._consumeData.bind(this));
    } else {
        if (this._binding.IsFinished()) {
            this._emitEnd();
        }
    }
};

YtReadableStream.prototype._emitEnd = function _emitEnd()
{
    "use strict";
    this.__DBG("_emitEnd");
    if (!this._ended) {
        this.emit("end");
        this.readable = false;
        this._ended = true;
    }
};

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
    if (!this._paused) {
        return;
    } else {
        this._paused = false;
        process.nextTick(this._consumeData.bind(this));
    }
};

YtReadableStream.prototype.destroy = function YtReadableStream$destroy()
{
    "use strict";
    this.__DBG("destroy");

    this._binding.Destroy();

    this.readable = false;
    this._paused = false;
    this._ended = true;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtReadableStream;
