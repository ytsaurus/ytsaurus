configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/config.h
    @ONLY
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/build.cpp.in
  ${CMAKE_CURRENT_BINARY_DIR}/build.cpp
)

add_library(yt-build ${CMAKE_CURRENT_BINARY_DIR}/build.cpp)

