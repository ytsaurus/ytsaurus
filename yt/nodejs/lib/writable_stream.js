var util = require("util");
var stream = require("stream");
var assert = require("assert");

var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __DBG;

if (process.env.NODE_DEBUG && /YTNODE/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Writable Stream:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

function YtWritableStream(low_watermark, high_watermark) {
    "use strict";

    if (__DBG.UUID) {
        this.__DBG  = function(x) { __DBG(this.__UUID + " -> " + x); };
        this.__UUID = __DBG.UUID.v4();
    } else {
        this.__DBG  = function(){};
    }

    this.__DBG("New");

    stream.Stream.call(this);

    this.readable = false;
    this.writable = true;

    this._ended = false;
    this._closed = false;

    var self = this;

    this._binding = new binding.TNodeJSInputStream(low_watermark, high_watermark);

    this._binding.on_drain = function() {
        self.__DBG("Bindings -> on_drain");
        if (!self._ended) {
            self.emit("drain");
        }
    };
}

util.inherits(YtWritableStream, stream.Stream);

YtWritableStream.prototype._emitClose = function() {
    "use strict";
    this.__DBG("_emitClose");
    if (!this._closed) {
        this.emit("close");
    }
    this._closed = true;
};

YtWritableStream.prototype.write = function(chunk, encoding) {
    "use strict";
    this.__DBG("write");

    if (typeof(chunk) !== "string" && !Buffer.isBuffer(chunk)) {
        throw new TypeError("Expected first argument to be a String or Buffer");
    }

    if (typeof(chunk) === "string") {
        chunk = new Buffer(chunk, encoding);
    }

    if (!this._ended /* && !this._closed */) {
        if (this._binding.Push(chunk, 0, chunk.length)) {
            return true;
        } else {
            this.__DBG("write -> (queue is full)");
            return false;
        }
    } else {
        return false;
    }
};

YtWritableStream.prototype.end = function(chunk, encoding) {
    "use strict";
    this.__DBG("end");
    if (chunk) {
        this.write(chunk, encoding);
    }
    this.destroySoon();
};

YtWritableStream.prototype.destroy = function() {
    "use strict";
    this.__DBG("destroy");

    this._binding.Destroy();

    this.writable = false;
    this._ended = true;
    this._closed = true;
};

YtWritableStream.prototype.destroySoon = function() {
    "use strict";
    this.__DBG("destroySoon");

    this._binding.End();

    this.writable = false;
    this._ended = true;

    var self = this;
    process.nextTick(function() { self._emitClose(); });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtWritableStream;
