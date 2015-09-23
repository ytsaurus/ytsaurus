#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/codec.cpp
  ${CMAKE_CURRENT_LIST_DIR}/details.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lz.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/snappy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/zlib.cpp
  ${CMAKE_CURRENT_LIST_DIR}/zstd.cpp
  ${CMAKE_CURRENT_LIST_DIR}/brotli.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/codec.h
  ${CMAKE_CURRENT_LIST_DIR}/details.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/lz.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/snappy.h
  ${CMAKE_CURRENT_LIST_DIR}/zlib.h
  ${CMAKE_CURRENT_LIST_DIR}/zstd.h
  ${CMAKE_CURRENT_LIST_DIR}/brotli.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/helpers.proto YT_SOURCE_FILES )
