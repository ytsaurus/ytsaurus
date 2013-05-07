// Middleware.
exports.YtApplicationApi = require("./lib/middleware/application_api").that;
exports.YtApplicationAuth = require("./lib/middleware/application_auth").that;

exports.YtAuthentication = require("./lib/middleware/authentication").that;
exports.YtMarkRequest = require("./lib/middleware/mark_request").that;
exports.YtLogRequest = require("./lib/middleware/log_request").that;

// Objects.
exports.YtAuthority = require("./lib/authority").that;
exports.YtDriver = require("./lib/driver").that;
exports.YtError = require("./lib/error").that;
exports.YtHostDiscovery = require("./lib/host_discovery").that;
exports.YtRegistry = require("./lib/registry").that;

// Utilities.
exports.Pause = require("./lib/utils").Pause;
exports.utils = require("./lib/utils");

exports.configureSingletons = function(config)
{
    var binding = require("./lib/ytnode");
    binding.ConfigureSingletons(config);
    process.on("exit", binding.ShutdownSingletons);
};
