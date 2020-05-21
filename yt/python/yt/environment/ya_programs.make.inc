INCLUDE(${ARCADIA_ROOT}/yt/yt/ya_programs.make.inc)

DEPENDS(
    yt/python/yt/environment/bin/yt_env_watcher_make
    yt/yt/packages/tests_package
    infra/nanny/logrotate
)
