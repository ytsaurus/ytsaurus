INCLUDE(${ARCADIA_ROOT}/${YT_ROOT}/yt/ya_programs.make.inc)

DEPENDS(
    yt/python/yt/environment/bin/yt_env_watcher_make
    ${YT_ROOT}/yt/packages/tests_package
    infra/nanny/logrotate
)
