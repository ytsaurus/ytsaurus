INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)
INCLUDE(${ARCADIA_ROOT}/${YT_ROOT}/yt/ya_programs.make.inc)

DEPENDS(
    ${YT_PYTHON_ROOT}/python/yt/environment/bin/yt_env_watcher_make
    # yt/packages/19_4
)
