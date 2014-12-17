#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/python )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/buffered_stream.cpp
  ${BASE}/descriptor.cpp
  ${BASE}/response.cpp
  ${BASE}/serialize.cpp
  ${BASE}/shutdown.cpp
  ${BASE}/stream.cpp
)

set( HDRS
  ${BASE}/common.h
  ${BASE}/buffered_stream.h
  ${BASE}/descriptor.h
  ${BASE}/response.h
  ${BASE}/serialize.h
  ${BASE}/shutdown.h
  ${BASE}/stream.h
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/yt
  ${CMAKE_BINARY_DIR}/include
  ${PYTHON_INCLUDE_DIR}
)

add_library( yt-driver-python SHARED
  ${BASE}/driver.cpp
  ${SRCS}
)

add_library( yt-yson-python SHARED
  ${BASE}/yson.cpp
  ${SRCS}
)

target_link_libraries( yt-driver-python
  ytlib
  ytext-pycxx
  ytext-lfalloc-fake
  ${PYTHON_LIBRARY}
)

target_link_libraries( yt-yson-python
  ytlib
  ytext-pycxx
  ytext-lfalloc-fake
  ${PYTHON_LIBRARY}
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-driver-python.so
  ${BASE}/driver/driver_lib.so
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-yson-python.so
  ${CMAKE_SOURCE_DIR}/python/yt_yson_bindings/yson_lib.so
)
