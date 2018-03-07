/**
 * Copyright (c) 2015 Trent Mick.
 * Copyright (c) 2015 Joyent Inc.
 * Copyright (c) 2015 Yandex.
 *
 * The bunyan logging library for node.js.
 * Patched by sandello@yandex-team.ru.
 *
 * -*- mode: js -*-
 * vim: expandtab:ts=4:sw=4
 */

var VERSION = '1.5.1-yandex';

/*
 * Bunyan log format version. This becomes the 'v' field on all log records.
 * This will be incremented if there is any backward incompatible change to
 * the log record format. Details will be in 'CHANGES.md' (the change log).
 */
var LOG_VERSION = 0;


var xxx = function xxx(s) {     // internal dev/debug logging
    var args = ['XX' + 'X: '+s].concat(
        Array.prototype.slice.call(arguments, 1));
    console.error.apply(this, args);
};
var xxx = function xxx() {};  // comment out to turn on debug logging


if (typeof (window) === 'undefined') {
    var os = require('os');
    var fs = require('fs');
    try {
        /* Use `+ ''` to hide this import from browserify. */
        var dtrace = require('dtrace-provider' + '');
    } catch (e) {
        dtrace = null;
    }
} else {
    var os = {
        hostname: function () {
            return window.location.host;
        }
    };
    var fs = {};
    var dtrace = null;
}
var util = require('util');
var assert = require('assert');
var EventEmitter = require('events').EventEmitter;

// Are we in the browser (e.g. running via browserify)?
var isBrowser = function () {
        return typeof (window) !== 'undefined'; }();

// XXX(sandello): Stubs.
var mv = null;
var sourceMapSupport = null;


//---- Internal support stuff

/**
 * A shallow copy of an object. Bunyan logging attempts to never cause
 * exceptions, so this function attempts to handle non-objects gracefully.
 */
function objCopy(obj) {
    if (obj == null) {  // null or undefined
        return obj;
    } else if (Array.isArray(obj)) {
        return obj.slice();
    } else if (typeof (obj) === 'object') {
        var copy = {};
        Object.keys(obj).forEach(function (k) {
            copy[k] = obj[k];
        });
        return copy;
    } else {
        return obj;
    }
}

var format = util.format;
if (!format) {
    // If node < 0.6, then use its `util.format`:
    // <https://github.com/joyent/node/blob/master/lib/util.js#L22>:
    var inspect = util.inspect;
    var formatRegExp = /%[sdj%]/g;
    format = function format(f) {
        var i, len;
        if (typeof (f) !== 'string') {
            var objects = [];
            for (i = 0, len = arguments.length; i < len; i++) {
                objects.push(inspect(arguments[i]));
            }
            return objects.join(' ');
        }

        var args = arguments;
        i = 1;
        len = args.length;
        var str = String(f).replace(formatRegExp, function (x) {
            if (i >= len)
                return x;
            switch (x) {
                case '%s': return String(args[i++]);
                case '%d': return Number(args[i++]);
                case '%j': return JSON.stringify(args[i++], safeCycles());
                case '%%': return '%';
                default:
                    return x;
            }
        });
        for (var x = args[i]; i < len; x = args[++i]) {
            if (x === null || typeof (x) !== 'object') {
                str += ' ' + x;
            } else {
                str += ' ' + inspect(x);
            }
        }
        return str;
    };
}


function _indent(s, indent) {
    if (!indent) indent = '    ';
    var lines = s.split(/\r?\n/g);
    return indent + lines.join('\n' + indent);
}


/**
 * Warn about an bunyan processing error.
 *
 * @param msg {String} Message with which to warn.
 * @param dedupKey {String} Optional. A short string key for this warning to
 *      have its warning only printed once.
 */
function _warn(msg, dedupKey) {
    assert.ok(msg);
    if (dedupKey) {
        if (_warned[dedupKey]) {
            return;
        }
        _warned[dedupKey] = true;
    }
    process.stderr.write(msg + '\n');
}
function _haveWarned(dedupKey) {
    return _warned[dedupKey];
}
var _warned = {};


function ConsoleRawStream() {}
ConsoleRawStream.prototype.write = function (rec) {
    if (rec.level < INFO) {
        console.log(rec);
    } else if (rec.level < WARN) {
        console.info(rec);
    } else if (rec.level < ERROR) {
        console.warn(rec);
    } else {
        console.error(rec);
    }
};


//---- Levels

var TRACE = 10;
var DEBUG = 20;
var INFO = 30;
var WARN = 40;
var ERROR = 50;
var FATAL = 60;

var levelFromName = {
    'trace': TRACE,
    'debug': DEBUG,
    'info': INFO,
    'warn': WARN,
    'error': ERROR,
    'fatal': FATAL
};
var nameFromLevel = {};
Object.keys(levelFromName).forEach(function (name) {
    nameFromLevel[levelFromName[name]] = name;
});

/**
 * Resolve a level number, name (upper or lowercase) to a level number value.
 *
 * @api public
 */
function resolveLevel(nameOrNum) {
    var level = (typeof (nameOrNum) === 'string'
            ? levelFromName[nameOrNum.toLowerCase()]
            : nameOrNum);
    return level;
}



//---- Logger class

/**
 * Create a Logger instance.
 *
 * @param options {Object} See documentation for full details. At minimum
 *    this must include a 'name' string key. Configuration keys:
 *      - `streams`: specify the logger output streams. This is an array of
 *        objects with these fields:
 *          - `type`: The stream type. See README.md for full details.
 *            Often this is implied by the other fields. Examples are
 *            'file', 'stream' and "raw".
 *          - `level`: Defaults to 'info'.
 *          - `path` or `stream`: The specify the file path or writeable
 *            stream to which log records are written. E.g.
 *            `stream: process.stdout`.
 *          - `closeOnExit` (boolean): Optional. Default is true for a
 *            'file' stream when `path` is given, false otherwise.
 *        See README.md for full details.
 *      - `level`: set the level for a single output stream (cannot be used
 *        with `streams`)
 *      - `stream`: the output stream for a logger with just one, e.g.
 *        `process.stdout` (cannot be used with `streams`)
 *    All other keys are log record fields.
 *
 * An alternative *internal* call signature is used for creating a child:
 *    new Logger(<parent logger>, <child options>[, <child opts are simple>]);
 *
 * @param _childSimple (Boolean) An assertion that the given `_childOptions`
 *    (a) only add fields (no config) and (b) no serialization handling is
 *    required for them. IOW, this is a fast path for frequent child
 *    creation.
 */
function Logger(options, _childOptions, _childSimple) {
    xxx('Logger start:', options);
    if (!(this instanceof Logger)) {
        return new Logger(options, _childOptions);
    }

    if (!options) {
        throw new TypeError('options (object) is required');
    }
    if (!options.name) {
        throw new TypeError('options.name (string) is required');
    }
    if (options.stream && options.streams) {
        throw new TypeError('cannot mix "streams" and "stream" options');
    }
    if (options.streams && !Array.isArray(options.streams)) {
        throw new TypeError('invalid options.streams: must be an array');
    }

    EventEmitter.call(this);

    // Start values.
    var self = this;
    this._level = DEBUG;
    this.streams = [];
    this.fields = {};

    // Handle *config* options (i.e. options that are not just plain data
    // for log records).
    if (options.stream) {
        self.addStream({
            type: 'stream',
            stream: options.stream,
            closeOnExit: false
        });
    } else if (options.streams) {
        options.streams.forEach(function (s) {
            self.addStream(s);
        });
    } else {
        if (isBrowser) {
            /*
             * In the browser we'll be emitting to console.log by default.
             * Any console.log worth its salt these days can nicely render
             * and introspect objects (e.g. the Firefox and Chrome console)
             * so let's emit the raw log record. Are there browsers for which
             * that breaks things?
             */
            self.addStream({
                type: 'raw',
                stream: new ConsoleRawStream(),
                closeOnExit: false
            });
        } else {
            self.addStream({
                type: 'stream',
                stream: process.stdout,
                closeOnExit: false
            });
        }
    }
    if (options.level) {
        this._level = resolveLevel(options.level);
    }
    xxx('Logger: ', self);

    // Fields.
    // These are the default fields for log records (minus the attributes
    // removed in this constructor). To allow storing raw log records
    // (unrendered), `this.fields` must never be mutated. Create a copy for
    // any changes.
    var fields = objCopy(options);
    delete fields.name;
    delete fields.stream;
    delete fields.level;
    delete fields.streams;
    delete fields.serializers;
    if (!fields.hostname) {
        fields.hostname = os.hostname();
    }
    if (!fields.pid) {
        fields.pid = process.pid;
    }
    Object.keys(fields).forEach(function (k) {
        self.fields[k] = fields[k];
    });
}

util.inherits(Logger, EventEmitter);


/**
 * Add a stream
 *
 * @param stream {Object}. Object with these fields:
 *    - `type`: The stream type. See README.md for full details.
 *      Often this is implied by the other fields. Examples are
 *      'file', 'stream' and "raw".
 *    - `path` or `stream`: The specify the file path or writeable
 *      stream to which log records are written. E.g.
 *      `stream: process.stdout`.
 *    - `level`: Optional. Falls back to `defaultLevel`.
 *    - `closeOnExit` (boolean): Optional. Default is true for a
 *      'file' stream when `path` is given, false otherwise.
 *    See README.md for full details.
 * @param defaultLevel {Number|String} Optional. A level to use if
 *      `stream.level` is not set. If neither is given, this defaults to INFO.
 */
Logger.prototype.addStream = function addStream(s, defaultLevel) {
    var self = this;
    if (defaultLevel === null || defaultLevel === undefined) {
        defaultLevel = DEBUG;
    }

    s = objCopy(s);

    // Implicit 'type' from other args.
    var type = s.type;
    if (!s.type) {
        if (s.stream) {
            s.type = 'stream';
        } else if (s.path) {
            s.type = 'file';
        }
    }
    s.raw = (s.type === 'raw');  // PERF: Allow for faster check in `_emit`.

    switch (s.type) {
    case 'stream':
        if (!s.closeOnExit) {
            s.closeOnExit = false;
        }
        break;
    case 'file':
        if (!s.stream) {
            s.stream = new BufferedFileStream(s.path);
            s.stream.on('error', function (err) {
                self.emit('error', err, s);
            });
            if (!s.closeOnExit) {
                s.closeOnExit = true;
            }
        } else {
            if (!s.closeOnExit) {
                s.closeOnExit = false;
            }
        }
        break;
    case 'raw':
        if (!s.closeOnExit) {
            s.closeOnExit = false;
        }
        break;
    default:
        throw new TypeError('unknown stream type "' + s.type + '"');
    }

    self.streams.push(s);
    delete self.haveNonRawStreams;  // reset
};

/**
 * A convenience method to reopen 'file' streams on a logger. This can be
 * useful with external log rotation utilities that move and re-open log files
 * (e.g. logrotate on Linux, logadm on SmartOS/Illumos). Those utilities
 * typically have rotation options to copy-and-truncate the log file, but
 * you may not want to use that. An alternative is to do this in your
 * application:
 *
 *      var log = bunyan.createLogger(...);
 *      ...
 *      process.on('SIGUSR2', function () {
 *          log.reopenFileStreams();
 *      });
 *      ...
 *
 * See <https://github.com/trentm/node-bunyan/issues/104>.
 */
Logger.prototype.reopenFileStreams = function () {
    var self = this;
    self.streams.forEach(function (s) {
        if (s.type === 'file') {
            if (s.stream) {
                // Not sure if typically would want this, or more immediate
                // `s.stream.destroy()`.
                s.stream.end();
                s.stream.destroySoon();
                delete s.stream;
            }
            s.stream = new BufferedFileStream(s.path);
            s.stream.on('error', function (err) {
                self.emit('error', err, s);
            });
        }
    });
};


/**
 * Get/set the level of all streams on this logger.
 *
 * Get Usage:
 *    // Returns the current log level (lowest level of all its streams).
 *    log.level() -> INFO
 *
 * Set Usage:
 *    log.level(INFO)       // set all streams to level INFO
 *    log.level('info')     // can use 'info' et al aliases
 */
Logger.prototype.level = function level(value) {
    if (value === undefined) {
        return this._level;
    }
    this._level = resolveLevel(value);
};

/**
 * Emit a log record.
 *
 * @param rec {log record}
 * @param noemit {Boolean} Optional. Set to true to skip emission
 *      and just return the JSON string.
 */
Logger.prototype._emit = function (rec, noemit) {
    var i;

    // Lazily determine if this Logger has non-'raw' streams. If there are
    // any, then we need to stringify the log record.
    if (this.haveNonRawStreams === undefined) {
        this.haveNonRawStreams = false;
        for (i = 0; i < this.streams.length; i++) {
            if (!this.streams[i].raw) {
                this.haveNonRawStreams = true;
                break;
            }
        }
    }

    // Stringify the object. Attempt to warn/recover on error.
    var str;
    if (noemit || this.haveNonRawStreams) {
        try {
            // XXX(sandello): Omit `safeCycles()` to save some CPU time.
            str = JSON.stringify(rec) + '\n';
        } catch (e) {
            var dedupKey = e.stack.split(/\n/g, 2).join('\n');
            _warn('bunyan: ERROR: Exception in '
                + '`JSON.stringify(rec)`. Record:\n'
                + _indent(format('%s\n%s', util.inspect(rec), e.stack)),
                dedupKey);
            str = format('(Exception in JSON.stringify(rec): %j. '
                + 'See stderr for details.)\n', e.message);
        }
    }

    if (noemit)
        return str;

    for (i = 0; i < this.streams.length; i++) {
        var s = this.streams[i];
        s.stream.write(s.raw ? rec : str);
    }

    return str;
};

Logger.prototype._logRawString = function (level, rec) {
    level = resolveLevel(level);
    for (i = 0; i < this.streams.length; i++) {
        var s = this.streams[i];
        if (this._level <= level) {
            s.stream.write(rec);
            s.stream.write("\n");
        }
    }
};


/**
 * Build a log emitter function for level msgLevel. I.e. this is the
 * creator of `log.info`, `log.error`, etc.
 */
function mkLogEmitter(msgLevel) {
    return function (fields, msg) {
        if (typeof(fields) === "string") {
            msg = fields;
            fields = {};
        }

        if (typeof(fields) === "undefined") {
            fields = {};
        }

        var rec = objCopy(this.fields);
        Object.keys(fields).forEach(function(k) {
            rec[k] = fields[k];
        });

        rec.message = msg;

        if (!rec.time) {
            rec.time = (new Date());
        }

        if (this._level <= msgLevel) {
            this._emit(rec);
        }
    };
}


/**
 * The functions below log a record at a specific level.
 *
 * Usages:
 *    log.<level>()  -> boolean is-trace-enabled
 *    log.<level>(<Error> err, [<string> msg, ...])
 *    log.<level>(<string> msg, ...)
 *    log.<level>(<object> fields, <string> msg, ...)
 *
 * where <level> is the lowercase version of the log level. E.g.:
 *
 *    log.info()
 *
 * @params fields {Object} Optional set of additional fields to log.
 * @params msg {String} Log message. This can be followed by additional
 *    arguments that are handled like
 *    [util.format](http://nodejs.org/docs/latest/api/all.html#util.format).
 */
Logger.prototype.trace = mkLogEmitter(TRACE);
Logger.prototype.debug = mkLogEmitter(DEBUG);
Logger.prototype.info = mkLogEmitter(INFO);
Logger.prototype.warn = mkLogEmitter(WARN);
Logger.prototype.error = mkLogEmitter(ERROR);
Logger.prototype.fatal = mkLogEmitter(FATAL);


// A JSON stringifier that handles cycles safely.
// Usage: JSON.stringify(obj, safeCycles())
function safeCycles() {
    var seen = [];
    return function (key, val) {
        if (!val || typeof (val) !== 'object') {
            return val;
        }
        if (seen.indexOf(val) !== -1) {
            return '[Circular]';
        }
        seen.push(val);
        return val;
    };
}


/**
 * BufferedFileStream buffers data to amortize calls to underlying file stream.
 */
 
function BufferedFileStream(path) {
    EventEmitter.call(this);
    this.writable = true;
    this.destroyed = false;

    this._underlying = fs.createWriteStream(path, {flags: 'a', encoding: 'utf8'});
    this._chunks = [];
    this._draining = false;

    var self = this;
    self._underlying.on('drain', function() {
        xxx("BFS: underlying stream emitted \"drain\" event");
        self._drainSoon();
    });
    self._underlying.on('error', function(err) {
        self.writable = false;
        self._chunks = [];
        self.emit('error', err);
    });
    self._underlying.on('close', function() {
        self.emit('close');
    });
}

util.inherits(BufferedFileStream, EventEmitter);

BufferedFileStream.prototype.write = function(chunk) {
    if (!this.writable)
        throw (new Error('BufferedFileStream has been ended already'));
    xxx("BFS: write()");
    this._chunks.push(chunk);
    this._drain();
    return false;
};

BufferedFileStream.prototype.end = function() {
    if (arguments.length > 0)
        this.write.apply(this, Array.prototype.slice.call(arguments));
    this.writable = false;
};

BufferedFileStream.prototype.destroy = function() {
    this.writable = false;
    this.destroyed = true;

    this._chunks = [];
    this._underlying.destroy();
};

BufferedFileStream.prototype.destroySoon = function() {
    this.writeable = false;
    this.destroyed = true;

    if (this._chunks.length > 0) {
        this._drain();
    }
    if (!this._draining) {
        xxx("BFS: calling destroySoon() on underlying stream");
        this._underlying.destroySoon();
    }
};

BufferedFileStream.prototype._drain = function() {
    if (this._draining) {
        xxx("BFS: already draining");
        return;
    }
    xxx("BFS: scheduled draining");
    this._draining = true;
    process.nextTick(this._drainSoon.bind(this));
};

BufferedFileStream.prototype._drainSoon = function() {
    var i = 0, n = this._chunks.length;
    if (n > 0) {
        var blob = "";
        for (; i < n; ++i) {
            blob += this._chunks[i];
        }
        this._underlying.write(blob);
        this._chunks = [];
        this._weight = 0;
        xxx("BFS: drained %d events", n);
    } else {
        this._draining = false;
        xxx("BFS: done draining");
        if (this.destroyed) {
            xxx("BFS: calling destroySoon() on underlying stream");
            this._underlying.destroySoon();
        }
    }
};

//---- Exports

module.exports = Logger;

module.exports.TRACE = TRACE;
module.exports.DEBUG = DEBUG;
module.exports.INFO = INFO;
module.exports.WARN = WARN;
module.exports.ERROR = ERROR;
module.exports.FATAL = FATAL;
module.exports.resolveLevel = resolveLevel;
module.exports.levelFromName = levelFromName;
module.exports.nameFromLevel = nameFromLevel;

module.exports.VERSION = VERSION;
module.exports.LOG_VERSION = LOG_VERSION;

module.exports.createLogger = function createLogger(options) {
    return new Logger(options);
};

module.exports.BufferedFileStream = BufferedFileStream;

// Useful for custom `type == 'raw'` streams that may do JSON stringification
// of log records themselves. Usage:
//    var str = JSON.stringify(rec, bunyan.safeCycles());
module.exports.safeCycles = safeCycles;
