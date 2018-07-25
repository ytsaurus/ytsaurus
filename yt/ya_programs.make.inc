INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

DEPENDS(
    ${YT_ROOT}/yt/node_modules
    ${YT_ROOT}/yt/nodejs/targets/bin
    ${YT_ROOT}/yt/nodejs/targets/package
    ${YT_ROOT}/yt/server/cell_master_program
    ${YT_ROOT}/yt/server/cell_node_program
    ${YT_ROOT}/yt/server/cell_proxy_program
    ${YT_ROOT}/yt/server/job_proxy_program
    ${YT_ROOT}/yt/server/exec_program
    ${YT_ROOT}/yt/server/tools_program
    ${YT_ROOT}/yt/server/programs/scheduler
    ${YT_ROOT}/yt/server/programs/controller_agent
)
