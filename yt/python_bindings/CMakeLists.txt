#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/python_bindings )
set( SRCS
    ${BASE}/driver.cpp
    ${BASE}/stream.cpp
    ${BASE}/serialize.cpp
)

set( HDRS
    ${BASE}/common.h
    ${BASE}/stream.h
    ${BASE}/serialize.h
)


include_directories(
  ${CMAKE_SOURCE_DIR}
  /usr/include/python2.7/
)


add_library( ytlib_python SHARED
  ${SRCS}
)

target_link_libraries( ytlib_python
  ytlib
  ytext-pycxx
)
