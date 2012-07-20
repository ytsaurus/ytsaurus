#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/actions )
set( SRCS
  ${BASE}/action_queue.cpp
  ${BASE}/action_queue_detail.cpp
  ${BASE}/callback_internal.cpp
  ${BASE}/cancelable_context.cpp
  ${BASE}/invoker_util.cpp
)
set( HDRS
  ${BASE}/action_queue.h
  ${BASE}/action_queue_detail.h
  ${BASE}/async_pipeline-inl.h
  ${BASE}/async_pipeline.h
  ${BASE}/bind.h
  ${BASE}/bind_helpers.h
  ${BASE}/bind_internal.h
  ${BASE}/bind_mpl.h
  ${BASE}/callback.h
  ${BASE}/callback_forward.h
  ${BASE}/callback_internal.h
  ${BASE}/callback_via.h
  ${BASE}/cancelable_context.h
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

PUMP( ${BASE}/bind.h.pump YT_SOURCE_FILES )
PUMP( ${BASE}/bind_internal.h.pump YT_SOURCE_FILES )
PUMP( ${BASE}/callback.h.pump YT_SOURCE_FILES )
PUMP( ${BASE}/callback_via.h.pump YT_SOURCE_FILES )
