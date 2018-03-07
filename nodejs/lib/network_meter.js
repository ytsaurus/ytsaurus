var Q = require("bluebird");
var fs = require("fs");

var YtReservoir = require("./reservoir.js").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("X", "Network");

var asyncReadDir = Q.promisify(fs.readdir);
var asyncReadFile = Q.promisify(fs.readFile);

function toString(x)
{
    return x.toString().replace(/\s+/, "");
}

function toFloat(x)
{
    return parseFloat(x.toString());
}

function YtNetworkMeter(logger, window_size)
{
    this.__DBG = __DBG.Tagged();

    this.logger = logger;
    this.state = null;
    this.reservoir = new YtReservoir(window_size);
    this.reservoir.push(0.0);

    this.load = {};
    this.coef = 0.0;
}

YtNetworkMeter.prototype._listIfaces = function YtNetworkMeter$_listIfaces()
{
    return asyncReadDir("/sys/class/net").then(function(files) {
        return files.filter(function(file) { return file.indexOf("eth") === 0; });
    });
};

YtNetworkMeter.prototype._getIface = function YtNetworkMeter$_getIface(iface)
{
    var path = "/sys/class/net/" + iface;
    var rx_bytes = asyncReadFile(path + "/statistics/rx_bytes").then(toFloat);
    var tx_bytes = asyncReadFile(path + "/statistics/tx_bytes").then(toFloat);
    var speed = asyncReadFile(path + "/speed").then(toFloat);
    var operstate = asyncReadFile(path + "/operstate").then(toString);
    return Q.all([rx_bytes, tx_bytes, speed, operstate]).catch(function() {});
};

YtNetworkMeter.prototype._getState = function YtNetworkMeter$_getState()
{
    var self = this;

    return self._listIfaces().then(function(ifaces) {
        var result = {_ts: new Date()};
        var i, n;
        for (i = 0, n = ifaces.length; i < n; ++i) {
            result[ifaces[i]] = self._getIface(ifaces[i]);
        }
        return Q.props(result);
    }).catch(function(err) {
        self.logger.debug("Failed to refresh network load: " + err.toString());
        return null;
    });
};

YtNetworkMeter.prototype.refresh = function YtNetworkMeter$refresh()
{
    var self = this;

    if (!/^linux/.test(process.platform)) {
        return Q.resolve(null);
    }

    return self._getState().then(function(state) {
        if (!self.state) {
            self.state = state;
            self.load = {};
            self.coef = 0.0;
            return;
        }

        var dt = state._ts - self.state._ts;

        var load = {};
        var coef = 0.0;
        for (var iface in state) {
            if (iface === "_ts" || !self.state[iface]) {
                continue;
            }
            var drx = state[iface][0] - self.state[iface][0];
            var dtx = state[iface][1] - self.state[iface][1];
            var speed = state[iface][2];
            var operstate = state[iface][3];
            if (operstate !== "up") {
                continue;
            }
            load[iface] = {
                speed: speed,
                rx_abs: (1000.0 / dt) * (8.0 * drx / 1024.0 / 1024.0) + 0.0,
                tx_abs: (1000.0 / dt) * (8.0 * dtx / 1024.0 / 1024.0) + 0.0,
            };
            load[iface].rx_rel = load[iface].rx_abs / speed;
            load[iface].tx_rel = load[iface].tx_abs / speed;
            load[iface].coef = Math.max(load[iface].rx_rel, load[iface].tx_rel);
            coef = Math.max(coef, load[iface].coef);
        }
        coef = Math.max(0.0, Math.min(1.0, coef));
        self.reservoir.push(coef);

        self.load = load;
        self.coef = self.reservoir.mean;
    });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtNetworkMeter;

