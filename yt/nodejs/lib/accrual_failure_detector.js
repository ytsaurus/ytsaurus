var events = require("events");

////////////////////////////////////////////////////////////////////////////////

var getHrtime = function()
{
    var now = process.hrtime();
    return Math.floor((now[0] * 1000) + (now[1] / 1000000));
};

var getLog10 = function(x)
{
    return Math.log(x) / Math.LN10;
};

var getNormalCdf = function(x, mean, stddev)
{
    var z = (x - mean) / stddev;
    return 1.0 / (1.0 + Math.exp(-z * (1.5976 + 0.070566 * z * z)));
};

////////////////////////////////////////////////////////////////////////////////

function Sample = function(window_size)
{
    this._window_size = window_size;
    this._window = [];

    this._sum = 0.0;
    this._sum_sq = 0.0;

    var self = this;
    Object.defineProperty(this, "length", {
        get: function() {
            return self._window.length;
        }
    });
    Object.defineProperty(this, "mean", {
        get: function() {
            return self._sum / self.length;
        }
    });
    Object.defineProperty(this, "variance", {
        get: function() {
            return self._sum_sq / self.length - self.mean * self.mean;
        }
    });
    Object.defineProperty(this, "stddev", {
        get: function() {
            return Math.sqrt(self.variance);
        }
    });
}

Sample.prototype.push = function(value)
{
    if (this._window.length >= this._window_size) {
        var dropped = this._window.shift();
        this._sum -= dropped;
        this._sum_sq -= dropped * dropped;
    }
    this._window.push(value);
    this._sum += value;
    this._sum_sq += value * value;
};

////////////////////////////////////////////////////////////////////////////////

function AccrualFailureDetector(
    window_size, phi_threshold, min_stddev,
    heartbeat_tolerance_ms, heartbeat_estimate_ms)
{
    this._sample = new Sample(window_size);
    this._last_at = null;

    this._phi_threshold = phi_threshold;
    this._min_stddev = min_stddev;
    this._heartbeat_tolerance_ms = heartbeat_tolerance_ms;
    this._heartbeat_estimate_ms = heartbeat_estimate_ms;

    events.EventEmitter.call(this);
}

util.inherits(AccrualFailureDetector, events.EventEmitter);

AccrualFailureDetector.prototype.heartbeat = function()
{
    var now = getHrtime();
    var before, after;

    before = this.phi() < this._phi_threshold;
    if (this._sample.length > 0) {
        this._sample.push(now - this._last_at);
    } else {
        // Bootstrap sample with initial estimate.
        var m = this._heartbeat_estimate_ms;
        var d = m / 4.0;
        this._sample.push(m - d);
        this._sample.push(m - d);
    }
    this._last_at = now;
    after = this.phi() < this._phi_threshold;

    if (this._sample.length < 5) {
        return;
    }

    if (before && !after) {
        this.emit("unavailable", this.phi());
    } else if (!before && after) {
        this.emit("available", this.phi());
    }
};

AccrualFailureDetector.prototype.phi = function()
{
    if (!this._last_at) {
        return 0.0;
    }

    var dt = getHrtime() - this._last_at;

    var est_mean = this._sample.mean + this._heartbeat_tolerance_ms;
    var est_stddev = Math.max(this._min_stddev, this._sample.stddev);

    return -getLog10(1.0 - getNormalCdf(dt, est_mean, est_stddev));
};
