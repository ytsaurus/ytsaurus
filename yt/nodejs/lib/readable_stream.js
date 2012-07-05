var util = require("util");
var stream = require("stream");
var assert = require("assert");

var binding = require("./ytnode");

////////////////////////////////////////////////////////////////////////////////

var __EOF = {};
var __DBG;

if (process.env.NODE_DEBUG && /YTNODE/.test(process.env.NODE_DEBUG)) {
    __DBG = function(x) { "use strict"; console.error("YT Readable Stream:", x); };
    __DBG.UUID = require("node-uuid");
} else {
    __DBG = function(){};
}

////////////////////////////////////////////////////////////////////////////////

function YtReadableStream() {
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
}

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._emitData = function(chunk) {
    "use strict";
    this.__DBG("_emitData");
    this.emit("data", chunk);
};

YtReadableStream.prototype._emitEnd = function() {
    "use strict";
    this.__DBG("_emitEnd");
    if (!this._ended) {
        this.emit("end");
    }
    this._ended = true;
};

YtReadableStream.prototype._emitQueue = function() {
    "use strict";
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
                    self.readable = false;
                }
            }
        });
    }
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
            if (self._paused || self._pending.length) {
                self._pending.push(__EOF);
            } else {
                assert.ok(self._pending.length === 0);
                self._emitEnd();
                self.readable = false;
            }
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
    this._paused = false;

    this._emitQueue();
};

YtReadableStream.prototype.destroy = function() {
    "use strict";
    this.__DBG("destroy");

    this._binding.Destroy();

    this.readable = false;
    this._ended = true;
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtReadableStream;
