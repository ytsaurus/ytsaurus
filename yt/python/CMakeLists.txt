set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/buffered_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/descriptor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/response.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/shutdown.cpp
  ${CMAKE_CURRENT_LIST_DIR}/stream.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/buffered_stream.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/descriptor.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/response.h
  ${CMAKE_CURRENT_LIST_DIR}/serialize.h
  ${CMAKE_CURRENT_LIST_DIR}/shutdown.h
  ${CMAKE_CURRENT_LIST_DIR}/stream.h
)

include_directories(
  ${PYTHON_INCLUDE_DIR}
)

add_library( yt-driver-python SHARED
  ${CMAKE_CURRENT_LIST_DIR}/driver.cpp
  ${SRCS}
)

add_library( yt-yson-python SHARED
  ${CMAKE_CURRENT_LIST_DIR}/yson.cpp
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

set( PYTHON_LINK_FLAGS "-static-libstdc++" )

if (NOT CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set( PYTHON_LINK_FLAGS "${PYTHON_LINK_FLAGS} -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/linker.map" )
endif()

set_target_properties( yt-driver-python 
  PROPERTIES
  LINK_FLAGS ${PYTHON_LINK_FLAGS}
)

set_target_properties( yt-yson-python 
  PROPERTIES
  LINK_FLAGS ${PYTHON_LINK_FLAGS}
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-driver-python.so
  ${CMAKE_SOURCE_DIR}/python/yt_driver_bindings/driver_lib.so
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-yson-python.so
  ${CMAKE_SOURCE_DIR}/python/yt_yson_bindings/yson_lib.so
)
