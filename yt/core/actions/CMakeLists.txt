#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/actions )
set( SRCS
  ${BASE}/callback_internal.cpp
  ${BASE}/cancelable_context.cpp
  ${BASE}/future.cpp
  ${BASE}/invoker_util.cpp
  ${BASE}/invoker_detail.cpp
)
set( HDRS
  ${BASE}/bind_helpers.h
  ${BASE}/bind_mpl.h
  ${BASE}/bind_internal.h
  ${BASE}/bind.h
  ${BASE}/callback.h
  ${BASE}/callback_internal.h
  ${BASE}/cancelable_context.h
  ${BASE}/future-inl.h
  ${BASE}/future.h
  ${BASE}/invoker.h
  ${BASE}/invoker_util.h
  ${BASE}/invoker_detail.h
  ${BASE}/signal.h
  ${BASE}/signal-inl.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
