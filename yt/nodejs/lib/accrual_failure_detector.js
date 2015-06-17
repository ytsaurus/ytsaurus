var events = require("events");
var util = require("util");

var YtReservoir = require("./reservoir").that;

////////////////////////////////////////////////////////////////////////////////

var getHrtime = function()
{
    var now = process.hrtime();
    return Math.floor((now[0] * 1000) + (now[1] / 1000000));
};

var getLog10 = function(x)
{
    return x === 0.0 ? -1000 : Math.log(x) / Math.LN10;
};

var getNormalCdf = function(x, mean, stddev)
{
    var z = (x - mean) / stddev;
    return 1.0 / (1.0 + Math.exp(-z * (1.5976 + 0.070566 * z * z)));
};

////////////////////////////////////////////////////////////////////////////////

function YtAccrualFailureDetector(
    window_size, phi_threshold, min_stddev,
    heartbeat_tolerance, heartbeat_estimate)
{
    "use strict";

    this.sample = new YtReservoir(window_size);
    this.last_at = null;

    this.phi_threshold = phi_threshold;
    this.min_stddev = min_stddev;
    this.heartbeat_tolerance_ms = heartbeat_tolerance;
    this.heartbeat_estimate_ms = heartbeat_estimate;

    events.EventEmitter.call(this);
}

util.inherits(YtAccrualFailureDetector, events.EventEmitter);

// XXX(sandello): Make sure to call either |heartbeat| or |heartbeatTS|.
// They capture time points on different time scales, so fusing them would
// result in a complete mess.

YtAccrualFailureDetector.prototype.heartbeat = function(now)
{
    "use strict";

    if (typeof(now) === "undefined") {
        now = getHrtime();
    }

    if (now < this.last_at) {
        return;
    }

    var before, after;

    before = (this.phi() < this.phi_threshold);
    if (this.sample.length > 0) {
        this.sample.push(now - this.last_at);
    } else {
        // Bootstrap sample with initial estimate.
        var m = this.heartbeat_estimate_ms;
        var d = m / 4.0;
        this.sample.push(m - d);
        this.sample.push(m - d);
    }
    this.last_at = now;
    after = (this.phi() < this.phi_threshold);

    if (this.sample.length < 5) {
        return;
    }

    if (before && !after) {
        this.emit("unavailable", this.phi());
    } else if (!before && after) {
        this.emit("available", this.phi());
    }
};

YtAccrualFailureDetector.prototype.heartbeatTS = function(date)
{
    "use strict";
    return this.heartbeat(+(date || new Date()));
};

YtAccrualFailureDetector.prototype.phi = function(now)
{
    "use strict";

    if (typeof(now) === "undefined") {
        now = getHrtime();
    }

    if (now < this.last_at) {
        return 0.0;
    }

    if (!this.last_at) {
        return 0.0;
    }

    var dt = now - this.last_at;

    var est_mean = this.sample.mean + this.heartbeat_tolerance_ms;
    var est_stddev = Math.max(this.min_stddev, this.sample.stddev);

    return -getLog10(1.0 - getNormalCdf(dt, est_mean, est_stddev));
};

YtAccrualFailureDetector.prototype.phiTS = function(date)
{
    "use strict";
    return this.phi(+(date || new Date()));
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtAccrualFailureDetector;
