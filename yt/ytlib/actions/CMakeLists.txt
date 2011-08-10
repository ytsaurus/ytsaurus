SET(YT_SOURCE_FILES
    action.cpp
    action-inl.h
    action_util.h
    action_queue.cpp
    prioritized_action_queue.cpp
    invoker.cpp
    async_result.h
    async_result-inl.h
    parallel_awaiter.h
    parallel_awaiter-inl.h

    ${SRCDIR}/action_util.h
)

ADD_CUSTOM_COMMAND(
    OUTPUT  ${SRCDIR}/action_util.h
    COMMAND ${PYTHON} ${ARCADIA_ROOT}/scripts/pump.py ${SRCDIR}/action_util.h.pump > ${SRCDIR}/action_util.h
    DEPENDS ${SRCDIR}/action_util.h.pump
    COMMENT "Regenerating action_util.h..."
)

