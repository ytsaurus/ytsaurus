var _ = require("underscore");

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

YtStatistics.prototype.dumpSolomon = function()
{
    var sensors = [];

    var keys = Object.keys(this.gauges);
    keys.sort();

    function parseLabels(key) {
        var labels = {};

        var previousString = "";
        var currentString = "";

        var spanOffset = -1;
        var spanLength = 0;

        var stateNone = 0;
        var stateSensor = 1;
        var stateKey = 2;
        var stateValue = 3;
        var state = stateSensor;

        for (var i = 0, n = key.length; i < n + 1; ++i) {
            if (i < n && key[i] !== ' ' && key[i] !== '=') {
                if (spanOffset === -1) {
                    spanOffset = i;
                }
                ++spanLength;
            } else {
                currentString = key.substr(spanOffset, spanLength);

                spanOffset = -1;
                spanLength = 0;

                switch (state) {
                    case stateSensor:
                        labels["sensor"] = currentString;
                        state = stateKey;
                        break;
                    case stateKey:
                        previousString = currentString;
                        state = stateValue;
                        break;
                    case stateValue:
                        labels[previousString] = currentString;
                        state = stateKey;
                        break;
                }
            }
        }

        return labels;
    }

    for (var i = 0, n = keys.length; i < n; ++i) {
        var labels = parseLabels(keys[i]);
        var gauge = this.gauges[keys[i]];
        if (typeof(gauge.counter) !== "undefined") {
            sensors.push({"labels": labels, "value": gauge.counter.toString(10), "mode": "deriv"});
        }
        if (typeof(gauge.digest) !== "undefined") {
            var quantiles = gauge.digest.percentile([0.5, 0.9, 0.95, 0.99, 1.0]);
            var suffixes = [".q50", ".q90", ".q95", ".q99", ".max"];
            var suffixesNumber = suffixes.length;

            sensors.push({"labels": _.clone(labels), "value": quantiles[0]});
            sensors.push({"labels": _.clone(labels), "value": quantiles[1]});
            sensors.push({"labels": _.clone(labels), "value": quantiles[2]});
            sensors.push({"labels": _.clone(labels), "value": quantiles[3]});
            sensors.push({"labels": _.clone(labels), "value": quantiles[4]});

            for (var m = sensors.length, k = m - suffixesNumber, l = 0; k < m; ++k, ++l) {
                sensors[k]["labels"]["sensor"] += suffixes[l];
            }
        }
        if (typeof(gauge.value) !== "undefined") {
            sensors.push({"labels": labels, "value": gauge.value});
        }
    }

    return JSON.stringify({"sensors": sensors});
}

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
