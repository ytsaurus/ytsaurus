var util = require("util");
var stream = require("stream");
var assert = require("assert");

var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("B", "Readable Stream");

////////////////////////////////////////////////////////////////////////////////

function YtReadableStream(watermark) {
    "use strict";
    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    this._pending = [];

    this._paused = false;
    this._ended = false;

    var self = this;

    this._binding = new binding.TOutputStreamWrap(watermark);
    this._binding.on_flowing = function() {
        self.__DBG("Bindings (OutputStream) -> on_flowing");
        self._consumeData();
    };

    this.__DBG = __DBG.Tagged(this._binding.cxx_id);
    this.__DBG("New");
}

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._consumeData = function() {
    "use strict";
    this.__DBG("_consumeData");

    if (!this.readable || this._paused || this._ended) {
        return;
    }

    var i, chunk, result = this._binding.Pull();

    this.__DBG("Bindings (OutputStream) -> Pull");

    for (i = 0; i < result.length; ++i) {
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
        this.emit("_drain");
    }
};

YtReadableStream.prototype._emitEnd = function() {
    "use strict";
    this.__DBG("_emitEnd");
    if (!this._ended) {
        this.emit("end");
    }
    this._ended = true;
};

YtReadableStream.prototype._endSoon = function() {
    "use strict";
    this.__DBG("_endSoon");

    if (!this.readable || this._ended) {
        return;
    }

    if (!this._binding.IsFlowing()) {
        var self = this;
        process.nextTick(function() {
            self.__DBG("_endSoon -> (inner-tick)");
            assert.ok(!self._binding.IsFlowing());
            self._emitEnd();
            self.readable = false;
        });
    } else {
        this.once("_drain", this._endSoon.bind(this));
    }
};

YtReadableStream.prototype.pause = function() {
    "use strict";
    this.__DBG("pause");
    this._paused = true;
};

YtReadableStream.prototype.resume = function() {
    "use strict";
    this.__DBG("resume");

    if (!this._paused) {
        return;
    } else {
        this._paused = false;
        process.nextTick(this._consumeData.bind(this));
    }
};

YtReadableStream.prototype.destroy = function() {
    "use strict";
    this.__DBG("destroy");

    this._binding.Destroy();

    this.readable = false;
    this._paused = false;
    this._ended = true;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtReadableStream;
