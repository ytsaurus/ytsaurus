// Register at-exit callback.
var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./lib/ytnode");
process.on("exit", function() {
    // Call _exit to avoid subtle shutdown issues.
    binding.WipeOutCurrentProcess(0);
});

// Ability to configure singletons.
exports.configureSingletons = binding.ConfigureSingletons;

// Middleware.
exports.YtApplicationApi = require("./lib/middleware/application_api").that;
exports.YtApplicationAuth = require("./lib/middleware/application_auth").that;
exports.YtApplicationHosts = require("./lib/middleware/application_hosts").that;
exports.YtApplicationUpravlyator = require("./lib/middleware/application_upravlyator").that;

exports.YtAcao = require("./lib/middleware/acao").that;
exports.YtAuthentication = require("./lib/middleware/authentication").that;
exports.YtLogRequest = require("./lib/middleware/log_request").that;
exports.YtLogSocket = require("./lib/middleware/log_socket").that;
exports.YtCheckPythonWrapperVersion = require("./lib/middleware/check_python_wrapper_version").that;

// Objects.
exports.YtAuthority = require("./lib/authority").that;
exports.YtCoordinator = require("./lib/coordinator").that;
exports.YtDriver = require("./lib/driver").that;
exports.YtError = require("./lib/error").that;
exports.YtRegistry = require("./lib/registry").that;
exports.YtEioWatcher = require("./lib/eio_watcher").that;
exports.YtStatistics = require("./lib/statistics").that;

// Utilities.
exports.Pause = require("./lib/utils").Pause;
exports.utils = require("./lib/utils");

