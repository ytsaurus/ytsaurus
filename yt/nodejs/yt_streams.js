var util    = require('util');
var stream  = require('stream');
var assert  = require('assert');
var binding = require('./build/Release/yt_test');

var __EOF   = {};

////////////////////////////////////////////////////////////////////////////////

function YtReadableStream() {
    stream.Stream.call(this);

    this.readable = true;
    this.writable = false;

    this._pending = [];
    this._paused = false;
    this._ended = false;

    var self = this;
    
    this._binding = new binding.TNodeJSOutputStream();
    this._binding.on_write = function(chunk) {
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
};

util.inherits(YtReadableStream, stream.Stream);

YtReadableStream.prototype._emitData = function(chunk) {
    this.emit('data', chunk);
};

YtReadableStream.prototype._emitEnd = function() {
    if (!this._ended) { 
        this.emit('end');
    }

    this.readable = false;
    this._ended = true;
}

YtReadableStream.prototype._emitQueue = function(callback) {
    if (this._pending.length) {
        var self = this;
        process.nextTick(function() {
            while (!self._paused && self._pending.length) {
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
    this._paused = true;
};

YtReadableStream.prototype.resume = function() {
    this._paused = false;
    this._emitQueue();
}

YtReadableStream.prototype.destroy = function() {
    this.readable = false;
    this._ended = true;
}

YtReadableStream.prototype.destroySoon = function() {
    var self = this;
    this._emitQueue(function() {
        self.readable = false;
        self._ended = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

function YtWritableStream() {
    stream.Stream.call(this);

    this.readable = false;
    this.writable = true;

    this._ended = false;

    var self = this;

    this._binding = new binding.TNodeJSInputStream();
};

util.inherits(YtWritableStream, stream.Stream);

YtWritableStream._emitClose() = function() {
    if (!this._ended) {
        this.emit('close');
    }

    this.writable = false;
    this._ended = true;
}

YtWritableStream.write = function(chunk, encoding) {
    if (typeof(chunk) !== 'string' && !Buffer.isBuffer(chunk)) {
        throw new TypeError('first argument must be a string or Buffer');
    }

    if (typeof(chunk) === 'string') {
        chunk = new Buffer(chunk, encoding);
    }

    if (!this._ended) {
        this._binding.Push(chunk, 0, chunk.length);
        return true;
    } else {
        return false;
    }
}

YtWritableStream.end = function(chunk, encoding) {
    if (chunk) {
        this.write(chunk, encoding);
    }

    this._binding.Close();

    this.writable = false;
    this._ended = true;

    this._emitClose();
}

YtWritableStream.destroy = function() {
    this._binding.Close();

    this.writable = false;
    this._ended = true;
}

YtWritableStream.destroySoon = function() {
    // TODO(sandello): Implement proper flush here.
    this._binding.Close();

    this.writable = false;
    this._ended = true;
}

////////////////////////////////////////////////////////////////////////////////

exports.YtWritableStream = YtWritableStream;
