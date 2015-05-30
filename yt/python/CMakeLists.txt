set( BASE ${CMAKE_SOURCE_DIR}/yt/python )
set( SRCS
  ${BASE}/buffered_stream.cpp
  ${BASE}/helpers.cpp
  ${BASE}/descriptor.cpp
  ${BASE}/response.cpp
  ${BASE}/serialize.cpp
  ${BASE}/shutdown.cpp
  ${BASE}/stream.cpp
)

set( HDRS
  ${BASE}/buffered_stream.h
  ${BASE}/helpers.h
  ${BASE}/descriptor.h
  ${BASE}/public.h
  ${BASE}/response.h
  ${BASE}/serialize.h
  ${BASE}/shutdown.h
  ${BASE}/stream.h
)

include_directories(
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

set_target_properties( yt-driver-python 
  PROPERTIES
  LINK_FLAGS "-static-libstdc++"
)

set_target_properties( yt-yson-python 
  PROPERTIES
  LINK_FLAGS "-static-libstdc++"
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-driver-python.so
  ${CMAKE_SOURCE_DIR}/python/yt/bindings/driver/driver_lib.so
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-yson-python.so
  ${CMAKE_SOURCE_DIR}/python/yt_yson_bindings/yson_lib.so
)
