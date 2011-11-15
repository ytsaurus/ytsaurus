SET(YT_SOURCE_FILES
    action.cpp
    action_util.h
    action-inl.h

    action_queue.cpp
    invoker.h
    invoker_util.cpp
    future.h
    future-inl.h
    parallel_awaiter.h
    parallel_awaiter-inl.h
    signal.h

    common.h

    action_util.h
    ${SRCDIR}/action_util-gen.h
)

ADD_CUSTOM_COMMAND(
    OUTPUT  ${SRCDIR}/action_util-gen.h
    COMMAND ${PYTHON} ${ARCADIA_ROOT}/scripts/pump.py ${SRCDIR}/action_util-gen.h.pump > ${SRCDIR}/action_util-gen.h
    DEPENDS ${SRCDIR}/action_util-gen.h.pump
    COMMENT "Regenerating action_util-gen.h..."
)

