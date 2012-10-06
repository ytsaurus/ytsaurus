var util = require("util");
var stream = require("stream");
var assert = require("assert");

var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YT(ALL|NODE)/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Readable Stream:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

function YtReadableStream(low_watermark, high_watermark) {
    "use strict";

    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG(this.__UUID + " -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function(){};
    }

    this.__DBG("New");

    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    this._pending = [];

    this._paused = false;
    this._ended = false;

    var self = this;

    this._binding = new binding.TNodeJSOutputStream(low_watermark, high_watermark);
    this._binding.on_data = function() {
        self.__DBG("Bindings (OutputStream) -> on_data");
        self._consumeData();
    };
}

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._consumeData = function() {
    "use strict";
    this.__DBG("_consumeData");

    if (!this.readable || this._paused || this._ended) {
        return;
    }

    var i, chunk, result = this._binding.Pull();

    if (typeof(result) === "undefined") {
        this.__DBG("Bindings (OutputStream) -> Pull <- undefined");
        return;
    } else {
        this.__DBG("Bindings (OutputStream) -> Pull <- " + result.length);
    }

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
        this._binding.Drain();
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

    if (this._binding.IsEmpty()) {
        var self = this;
        process.nextTick(function() {
            self.__DBG("_endSoon -> (inner-tick)");
            assert.ok(self._binding.IsEmpty());
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
