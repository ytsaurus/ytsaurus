INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

DEPENDS(
    ${YT_ROOT}/yt/server/cell_master/bin
    ${YT_ROOT}/yt/server/cell_node/bin
    ${YT_ROOT}/yt/server/rpc_proxy/bin
    ${YT_ROOT}/yt/server/job_proxy/bin
    ${YT_ROOT}/yt/server/bin/exec
    ${YT_ROOT}/yt/server/bin/tools
    ${YT_ROOT}/yt/server/scheduler/bin
    ${YT_ROOT}/yt/server/controller_agent/bin
)
