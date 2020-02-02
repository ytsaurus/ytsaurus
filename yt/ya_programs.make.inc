INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

DEPENDS(
    ${YT_ROOT}/yt/server/all
    ${YT_ROOT}/yt/server/all/stripped
    ${YT_ROOT}/yt/server/clock_server/bin
    ${YT_ROOT}/yt/server/controller_agent/bin
    ${YT_ROOT}/yt/server/exec/bin
    ${YT_ROOT}/yt/server/http_proxy/bin
    ${YT_ROOT}/yt/server/job_proxy/bin
    ${YT_ROOT}/yt/server/master/bin
    ${YT_ROOT}/yt/server/node/bin
    ${YT_ROOT}/yt/server/rpc_proxy/bin
    ${YT_ROOT}/yt/server/scheduler/bin
    ${YT_ROOT}/yt/server/tools/bin
)
