configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/config.h
  @ONLY
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/build.cpp.in
  ${CMAKE_CURRENT_BINARY_DIR}/build.cpp
  @ONLY
)

add_library(yp-build ${CMAKE_CURRENT_BINARY_DIR}/build.cpp)
