var YtRegistry = require("../registry").that;
var YtError = require("../error").that;

var Q = require("bluebird");

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

    var parsed_banned_config = [];
    if (banned_config) {
        for (var i = 0, len = banned_config.length; i < len; ++i) {
            range = banned_config[i];
            parsed_banned_config.push([parseVersion(range[0]), parseVersion(range[1])]);
        }
    }

    return function(req, rsp, next) {
        var ua = req.headers["user-agent"] + ""; // Do not care about null or undefined.
        var re = ua.match(/^Python wrapper \b(\d+)\.(\d+)\.(\d+)\b/);
        var version = re && [parseInt(re[1]), parseInt(re[2]), parseInt(re[3])];
        var error;
        var range;

        try {
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

            if (version) {
                for (var i = 0, len = parsed_banned_config.length; i < len; ++i) {
                    range = parsed_banned_config[i];

                    if (utils.lexicographicalCompare(version, range[0]) >= 0 &&
                        utils.lexicographicalCompare(version, range[1]) < 0)
                    {
                        error = new YtError(
                            ("You are using banned version of `yandex-yt-python` ({} <= {} < {}); " +
                            "please consider upgrading")
                                .format(printVersion(range[0]), printVersion(version), printVersion(range[1])));
                        (req.logger || logger).debug(
                            "Client is using banned version of yandex-yt-python",
                            {
                                version: printVersion(version),
                                banned_range: [printVersion(range[0]), printVersion(range[1])],
                            });
                        rsp.statusCode = 402;
                        return void utils.dispatchAs(rsp, error.toJson(), "application/json");
                    }
                }
            }

            next();
        } catch (err) {
            rsp.statusCode = 400;
            var error = YtError.ensureWrapped(err);
            return utils.dispatchAs(rsp, error.toJson(), "application/json");
        }
    };
};

