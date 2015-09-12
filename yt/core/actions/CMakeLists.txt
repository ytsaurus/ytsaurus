#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/callback_internal.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cancelable_context.cpp
  ${CMAKE_CURRENT_LIST_DIR}/future.cpp
  ${CMAKE_CURRENT_LIST_DIR}/invoker_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/invoker_util.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/bind.h
  ${CMAKE_CURRENT_LIST_DIR}/bind_helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/bind_internal.h
  ${CMAKE_CURRENT_LIST_DIR}/bind_mpl.h
  ${CMAKE_CURRENT_LIST_DIR}/callback.h
  ${CMAKE_CURRENT_LIST_DIR}/callback_internal.h
  ${CMAKE_CURRENT_LIST_DIR}/cancelable_context-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/cancelable_context.h
  ${CMAKE_CURRENT_LIST_DIR}/future-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/future.h
  ${CMAKE_CURRENT_LIST_DIR}/invoker-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/invoker.h
  ${CMAKE_CURRENT_LIST_DIR}/invoker_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/invoker_util.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/signal-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/signal.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
