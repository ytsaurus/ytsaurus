#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/python/yt/driver )
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
    ${CMAKE_SOURCE_DIR}/yt
    ${CMAKE_BINARY_DIR}
    ${CMAKE_BINARY_DIR}/yt
    ${CMAKE_BINARY_DIR}/include
    /usr/include/python2.7/
)


add_library( ytlib_python SHARED
    ${SRCS}
)

target_link_libraries( ytlib_python
    ytlib
    ytext-pycxx
)
