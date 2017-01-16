IF(YMAKE)
  SET(YT_ABI 19_1)
ENDIF()

################################################################################
IF(YMAKE)
################################################################################

UNION()

OWNER(snowball sandello)

SRCDIR(yt/${YT_ABI}/yt/nodejs)

FILES(
  bin/yt_http_proxy
  index.js
  lib/accrual_failure_detector.js
  lib/application_auth.js
  lib/application_hosts.js
  lib/application_operations.js
  lib/application_upravlyator.js
  lib/application_versions.js
  lib/authentication.js
  lib/authority.js
  lib/bunyan.js
  lib/command.js
  lib/coordinator.js
  lib/debug.js
  lib/driver_facade_v2.js
  lib/driver_facade_v3.js
  lib/driver.js
  lib/eio_watcher.js
  lib/error.js
  lib/external_services.js
  lib/http_request.js
  lib/master.js
  lib/middleware/acao.js
  lib/middleware/application_api.js
  lib/middleware/application_auth.js
  lib/middleware/application_hosts.js
  lib/middleware/application_upravlyator.js
  lib/middleware/authentication.js
  lib/middleware/check_python_wrapper_version.js
  lib/middleware/log_request.js
  lib/middleware/log_socket.js
  lib/middleware/min_python_wrapper_version.js
  lib/readable_stream.js
  lib/registry.js
  lib/reservoir.js
  lib/statistics.js
  lib/utils.js
  lib/worker.js
  lib/writable_stream.js
  static/auth-error.mustache
  static/auth-index.mustache
  static/auth-layout.mustache
  static/auth-token.mustache
  static/bootstrap.min.css
)

END()

################################################################################
ENDIF()
################################################################################

