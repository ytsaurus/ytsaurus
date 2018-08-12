INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

DEPENDS(
    ${YT_ROOT}/yt/node_modules
    ${YT_ROOT}/yt/nodejs/targets/bin
    ${YT_ROOT}/yt/nodejs/targets/package
    ${YT_ROOT}/yt/server/bin/master
    ${YT_ROOT}/yt/server/bin/node
    ${YT_ROOT}/yt/server/bin/rpc_proxy
    ${YT_ROOT}/yt/server/bin/job_proxy
    ${YT_ROOT}/yt/server/bin/exec
    ${YT_ROOT}/yt/server/bin/tools
    ${YT_ROOT}/yt/server/bin/scheduler
    ${YT_ROOT}/yt/server/bin/controller_agent
)
