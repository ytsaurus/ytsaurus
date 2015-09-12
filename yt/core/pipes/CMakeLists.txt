#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/async_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/async_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/io_dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/io_dispatcher_impl.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipe.cpp

)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/async_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/async_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/io_dispatcher.h
  ${CMAKE_CURRENT_LIST_DIR}/io_dispatcher_impl.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/pipe.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
