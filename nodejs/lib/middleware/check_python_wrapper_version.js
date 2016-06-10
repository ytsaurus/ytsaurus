var YtRegistry = require("../registry").that;
var YtError = require("../error").that;

var utils = require("../utils");

////////////////////////////////////////////////////////////////////////////////

function printVersion(v) {
    return v.join(".");
}

exports.that = function Middleware__YtCheckPythonWrapperVersion()
{
    var min_config = YtRegistry.get("config", "min_python_wrapper_version");
    var banned_config = YtRegistry.get("config", "banned_python_wrapper_versions");
    var logger = YtRegistry.get("logger");

    function parseVersion(text) {
        var re = text.match(/^(\d+)\.(\d+)\.(\d+)$/);
        return [parseInt(re[1]), parseInt(re[2]), parseInt(re[3])];
    }

    var min_version = [ min_config.major, min_config.minor, min_config.patch ];

    return function(req, rsp, next) {
        var ua = req.headers["user-agent"] + ""; // Do not care about null or undefined.
        var re = ua.match(/^Python wrapper \b(\d+)\.(\d+)\.(\d+)\b/);
        var version = re && [parseInt(re[1]), parseInt(re[2]), parseInt(re[3])];
        var error;
        var range;

        if (min_config.enable &&
            version &&
            utils.lexicographicalCompare(version, min_version) < 0)
        {
            error = new YtError(
                "You are using deprecated version of `yandex-yt-python` ({} < {}); please consider upgrading"
                    .format(printVersion(version), printVersion(min_version)));

            (req.logger || logger).debug(
                "Client is using deprecated version of yandex-yt-python",
                {
                    version: printVersion(version),
                    min_version: printVersion(min_version),
                });
            rsp.statusCode = 402;
            return void utils.dispatchAs(rsp, error.toJson(), "application/json");
        }

        if (banned_config && version) {
            for (var i = 0, len = banned_config.length; i < len; ++i) {
                range = banned_config[i];

                if (utils.lexicographicalCompare(version, parseVersion(range[0])) >= 0 &&
                    utils.lexicographicalCompare(version, parseVersion(range[1])) < 0)
                {
                    error = new YtError(
                        ("You are using banned version of `yandex-yt-python` ({} <= {} < {}); " +
                        "please consider upgrading")
                            .format(range[0], printVersion(version), range[1]));
                    (req.logger || logger).debug(
                        "Client is using banned version of yandex-yt-python",
                        {
                            version: printVersion(version),
                            banned_range: range,
                        });
                    rsp.statusCode = 402;
                    return void utils.dispatchAs(rsp, error.toJson(), "application/json");
                }
            }
        }

        next();
    };
};

