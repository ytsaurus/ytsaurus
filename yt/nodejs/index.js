// Middleware.
exports.YtApplicationApi = require("./lib/middleware/application_api").that;
exports.YtApplicationAuth = require("./lib/middleware/application_auth").that;
exports.YtApplicationHosts = require("./lib/middleware/application_hosts").that;
exports.YtApplicationUpravlyator = require("./lib/middleware/application_upravlyator").that;

exports.YtAcao = require("./lib/middleware/acao").that;
exports.YtAuthentication = require("./lib/middleware/authentication").that;
exports.YtIsolateRequest = require("./lib/middleware/isolate_request").that;
exports.YtLogRequest = require("./lib/middleware/log_request").that;
exports.YtLogSocket = require("./lib/middleware/log_socket").that;

// Objects.
exports.YtAuthority = require("./lib/authority").that;
exports.YtCoordinator = require("./lib/coordinator").that;
exports.YtDriver = require("./lib/driver").that;
exports.YtError = require("./lib/error").that;
exports.YtRegistry = require("./lib/registry").that;

// Utilities.
exports.Pause = require("./lib/utils").Pause;
exports.utils = require("./lib/utils");

exports.configureSingletons = function(config)
{
    var binding = require("./lib/ytnode");
    process.on("exit", binding.ShutdownSingletons);
    binding.ConfigureSingletons(config);
};
