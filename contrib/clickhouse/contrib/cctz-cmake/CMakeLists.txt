# This file was generated by the YaTool build system (https://github.com/yandex/yatool),
# from a source YaTool build configuration provided in ya.make files.
#
# If the repository supports both CMake and ya build configurations, please modify both of them.
#
# If only CMake build configuration is supported then modify only CMake files and note that only
# simple modifications are allowed like adding source-files to targets or adding simple properties
# like target_include_directories. These modifications will be ported to original ya.make files
# by maintainers. Any complex modifications which can't be easily ported back to the ya build
# system may be rejected.
#
# Please refer to the build instructions in the repository for more information about manual
# changes in this file.

find_package(linux-headers-generic REQUIRED)

add_library(clickhouse-contrib-cctz-cmake)


target_compile_options(clickhouse-contrib-cctz-cmake PUBLIC
  -DOS_LINUX
)

target_compile_options(clickhouse-contrib-cctz-cmake PRIVATE
  -DINCBIN_SILENCE_BITCODE_WARNING
  -D_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS
  -D_LIBUNWIND_IS_NATIVE_ONLY
  $<IF:$<CXX_COMPILER_ID:MSVC>,,-Wno-everything>
)

target_include_directories(clickhouse-contrib-cctz-cmake PRIVATE
  ${PROJECT_SOURCE_DIR}/library/cpp/clickhouse_deps/incbin_stub
)

target_link_libraries(clickhouse-contrib-cctz-cmake PUBLIC
  linux-headers-generic::linux-headers-generic
  contrib-libs-cxxsupp
  cpp-clickhouse_deps-incbin_stub
)

target_sources(clickhouse-contrib-cctz-cmake PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/contrib/cctz-cmake/TimeZones.generated.cpp
)

