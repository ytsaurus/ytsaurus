
# This file was generated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.

add_subdirectory(ut)
add_library(cpp-mapreduce-common)


target_compile_options(cpp-mapreduce-common PRIVATE
  -Wdeprecated-this-capture
)

target_link_libraries(cpp-mapreduce-common PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
  library-cpp-svnversion
  cpp-threading-future
  library-cpp-yson
  cpp-yson-json
  cpp-yson-node
  cpp-mapreduce-interface
  mapreduce-interface-logging
)

target_sources(cpp-mapreduce-common PRIVATE
  ${PROJECT_SOURCE_DIR}/yt/cpp/mapreduce/common/debug_metrics.cpp
  ${PROJECT_SOURCE_DIR}/yt/cpp/mapreduce/common/helpers.cpp
  ${PROJECT_SOURCE_DIR}/yt/cpp/mapreduce/common/retry_lib.cpp
  ${PROJECT_SOURCE_DIR}/yt/cpp/mapreduce/common/wait_proxy.cpp
)






