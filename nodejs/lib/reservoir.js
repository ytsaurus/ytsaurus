function YtReservoir(window_size)
{
    "use strict";

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

YtReservoir.prototype.push = function(value)
{
    "use strict";

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

exports.that = YtReservoir;
