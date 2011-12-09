#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/actions )
set( SRCS
  ${BASE}/action.cpp
  ${BASE}/action_queue.cpp
  ${BASE}/invoker_util.cpp
)
set( HDRS
  ${BASE}/action-inl.h
  ${BASE}/action.h
  ${BASE}/action_queue.h
  ${BASE}/action_util-gen.h
  ${BASE}/action_util.h
  ${BASE}/common.h
  ${BASE}/future-inl.h
  ${BASE}/future.h
  ${BASE}/invoker.h
  ${BASE}/invoker_util.h
  ${BASE}/parallel_awaiter-inl.h
  ${BASE}/parallel_awaiter.h
  ${BASE}/signal.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}

  ${BASE}/action_util-gen.h
)

add_custom_command(
    OUTPUT  ${BASE}/action_util-gen.h
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/pump.py ${BASE}/action_util-gen.h.pump > ${BASE}/action_util-gen.h
    DEPENDS ${BASE}/action_util-gen.h.pump
    COMMENT "Regenerating action_util-gen.h..."
)
