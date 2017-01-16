var UI64 = require("cuint").UINT64;
var TDigest = require("tdigest").TDigest;

////////////////////////////////////////////////////////////////////////////////

function computeKey(metric, tags)
{
    var parts = [metric];
    if (tags) {
        var keys = Object.keys(tags);
        keys.sort();
        for (var i = 0, n = keys.length; i < n; ++i) {
            parts.push("" + keys[i] + "=" + tags[keys[i]]);
        }
    }
    return parts.join(" ");
}

function YtStatistics(ttl)
{
    "use strict";

    this.gauges = {};

    if (ttl) {
        this.ttl = ttl;
        setInterval(this.clearExpiredGauges.bind(this), ttl);
    }
}

YtStatistics.prototype.getGauge = function(metric, tags)
{
    var key = computeKey(metric, tags);
    if (typeof(this.gauges[key]) === "undefined") {
        this.gauges[key] = {};
    }
    this.gauges[key].ts = new Date();
    return this.gauges[key];
};

YtStatistics.prototype.clearExpiredGauges = function()
{
    var now = new Date();
    var keys = Object.keys(this.gauges);
    for (var i = 0, n = keys.length; i < n; ++i) {
        var key = keys[i];
        var gauge = this.gauges[key];
        if (this.ttl && (now - gauge.ts) > this.ttl) {
            delete this.gauges[key];
        }
    }
};

YtStatistics.prototype.inc = function(metric, tags, value)
{
    var gauge = this.getGauge(metric, tags);
    if (typeof(gauge.counter) === "undefined") {
        gauge.counter = UI64(0);
    }
    if (typeof(value) === "object") {
        gauge.counter.add(value);
    } else {
        if (value >= 0) {
            gauge.counter.add(UI64(value));
        } else {
            gauge.counter.subtract(UI64(-value));
        }
    }
};

YtStatistics.prototype.upd = function(metric, tags, value)
{
    var gauge = this.getGauge(metric, tags);
    if (typeof(gauge.digest) === "undefined") {
        gauge.digest = new TDigest(0.33, 15, 1.15);
    }
    if (typeof(value) === "object") {
        gauge.digest.push_centroid(value);
        gauge.digest.compress();
    } else {
        gauge.digest.push(value);
    }
};

YtStatistics.prototype.set = function(metric, tags, value)
{
    var gauge = this.getGauge(metric, tags);
    gauge.value = value;
};

YtStatistics.prototype.dump = function()
{
    var result = [];
    var keys = Object.keys(this.gauges);
    keys.sort();

    function patchName(key, suffix) {
        if (key.indexOf(" ") >= 0) {
            return key.replace(" ", suffix + " ");
        } else {
            return key + suffix;
        }
    }

    for (var i = 0, n = keys.length; i < n; ++i) {
        var key = keys[i];
        var gauge = this.gauges[key];
        if (typeof(gauge.counter) !== "undefined") {
            result.push(key + " " + gauge.counter.toString(10));
        }
        if (typeof(gauge.digest) !== "undefined") {
            var quantiles = gauge.digest.percentile([0.5, 0.9, 0.95, 0.99, 1.0]);
            result.push(patchName(key, ".q50") + " " + quantiles[0]);
            result.push(patchName(key, ".q90") + " " + quantiles[1]);
            result.push(patchName(key, ".q95") + " " + quantiles[2]);
            result.push(patchName(key, ".q99") + " " + quantiles[3]);
            result.push(patchName(key, ".max") + " " + quantiles[4]);
        }
        if (typeof(gauge.value) !== "undefined") {
            result.push(key + " " + gauge.value.toString());
        }
    }
    return result.join("\n");
};

YtStatistics.prototype.mergeTo = function(other)
{
    var keys = Object.keys(this.gauges);
    keys.sort();
    for (var i = 0, n = keys.length; i < n; ++i) {
        var key = keys[i];
        var gauge = this.gauges[key];
        if (typeof(gauge.counter) !== "undefined") {
            other.inc(key, null, gauge.counter);
        }
        if (typeof(gauge.digest) !== "undefined") {
            other.upd(key, null, gauge.digest.toArray());
        }
        if (typeof(gauge.value) !== "undefined") {
            other.set(key, null, gauge.value);
        }
    }
    this.gauges = {};
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtStatistics;
