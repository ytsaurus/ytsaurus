SET(PROTO_NAMESPACE yt/${YT_ABI})

include(Sources.txt)

add_library(yt-core-test-framework
  ${YT_SRCS}
)

target_include_directories(yt-core-test-framework
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/gtest
  ${CMAKE_SOURCE_DIR}/contrib/libs/gmock
)

target_link_libraries(yt-core-test-framework
  yt-core
  deps-arcadia-library-lfalloc
  deps-arcadia-contrib-libs-gtest
  deps-arcadia-contrib-libs-gmock
)

SET(YT_UNITTESTS_MAIN ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
  CACHE INTERNAL "Name of main.cpp file for gtests framework" FORCE
)
