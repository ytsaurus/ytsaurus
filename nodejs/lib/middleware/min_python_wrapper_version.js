var YtRegistry = require("../registry").that;
var YtError = require("../error").that;

var utils = require("../utils");

////////////////////////////////////////////////////////////////////////////////

function printVersion(v) {
    return v.join(".");
}

exports.that = function Middleware__YtMinPythonWrapperVersion()
{
    var config = YtRegistry.get("config", "min_python_wrapper_version");
    var logger = YtRegistry.get("logger");

    if (!config.enable) {
        return function(req, rsp, next) {
            next();
        };
    }

    var min_version = [ config.major, config.minor, config.patch ];

    return function(req, rsp, next) {
        var ua = req.headers["user-agent"] + ""; // Do not care about null or undefined.
        var re = ua.match(/^Python wrapper \b(\d+)\.(\d+)\.(\d+)\b/);
        if (re) {
            var version = [parseInt(re[1]), parseInt(re[2]), parseInt(re[3])];
            if (utils.lexicographicalCompare(version, min_version) < 0) {
                var error = new YtError(
                    "You are using deprecated version of `yandex-yt-python` " +
                    "(" + printVersion(version) + " < " + printVersion(min_version) + "); " +
                    "please consider upgrading");
                (req.logger || logger).debug(
                    "Client is using deprecated version of yandex-yt-python",
                    {
                        version: printVersion(version),
                        min_version: printVersion(min_version),
                    });
                rsp.statusCode = 402;
                return void utils.dispatchAs(rsp, error.toJson(), "application/json");
            }
        }
        next();
    };
};

