#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/python/yt/driver )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/buffered_stream.cpp
  ${BASE}/descriptor.cpp
  ${BASE}/response.cpp
  ${BASE}/serialize.cpp
  ${BASE}/stream.cpp
)

set( HDRS
  ${BASE}/common.h
  ${BASE}/buffered_stream.h
  ${BASE}/descriptor.h
  ${BASE}/response.h
  ${BASE}/serialize.h
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


add_library( ytlib_python SHARED
    ${BASE}/driver.cpp
    ${SRCS}
)

add_library( yson_python SHARED
    ${BASE}/yson.cpp
    ${SRCS}
)

target_link_libraries( ytlib_python
    ytlib
    ytext-pycxx
    ${PYTHON_LIBRARY}
)

target_link_libraries( yson_python
    ytlib
    ytext-pycxx
    ${PYTHON_LIBRARY}
)
