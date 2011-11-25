SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action.cpp
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_util.h
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action-inl.h

  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_queue.cpp
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/invoker.h
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/invoker_util.cpp
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/future.h
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/future-inl.h
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/parallel_awaiter.h
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/parallel_awaiter-inl.h
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/signal.h

  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/common.h

  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_util.h
  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_util-gen.h
)

ADD_CUSTOM_COMMAND(
    OUTPUT  ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_util-gen.h
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/pump.py ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_util-gen.h.pump > ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_util-gen.h
    DEPENDS ${CMAKE_SOURCE_DIR}/yt/ytlib/actions/action_util-gen.h.pump
    COMMENT "Regenerating action_util-gen.h..."
)
