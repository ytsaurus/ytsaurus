#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/actions )
set( SRCS
  ${BASE}/action.cpp
  ${BASE}/action_queue.cpp
  ${BASE}/callback_internal.cpp
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
)

PUMP( ${BASE}/action_util-gen.h.pump YT_SOURCE_FILES )
PUMP( ${BASE}/bind.h.pump YT_SOURCE_FILES )
PUMP( ${BASE}/bind_internal.h.pump YT_SOURCE_FILES )
PUMP( ${BASE}/callback.h.pump YT_SOURCE_FILES )