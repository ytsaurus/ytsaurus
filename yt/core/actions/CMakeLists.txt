#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/actions )
set( SRCS
  ${BASE}/callback_internal.cpp
  ${BASE}/cancelable_context.cpp
  ${BASE}/future.cpp
  ${BASE}/invoker_util.cpp
)
set( HDRS
  ${BASE}/bind_helpers.h
  ${BASE}/bind_mpl.h
  ${BASE}/callback_forward.h
  ${BASE}/callback_internal.h
  ${BASE}/cancelable_context.h
  ${BASE}/common.h
  ${BASE}/future-inl.h
  ${BASE}/future.h
  ${BASE}/invoker.h
  ${BASE}/invoker_util.h
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
