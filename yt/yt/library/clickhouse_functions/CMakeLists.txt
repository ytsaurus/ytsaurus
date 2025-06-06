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

add_library(yt-library-clickhouse_functions)


target_compile_options(yt-library-clickhouse_functions PRIVATE
  -Wdeprecated-this-capture
  -Wimplicit-fallthrough
  -Wparentheses
  -Wno-logical-op-parentheses
  -Wno-bitwise-op-parentheses
)

target_include_directories(yt-library-clickhouse_functions PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/src
  ${PROJECT_SOURCE_DIR}/contrib/libs/double-conversion
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/Foundation/include
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/Net/include
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/Util/include
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/XML/include
  ${PROJECT_SOURCE_DIR}/contrib/libs/re2
  ${PROJECT_SOURCE_DIR}/contrib/libs/rapidjson/include
)

target_link_libraries(yt-library-clickhouse_functions PUBLIC
  linux-headers-generic::linux-headers-generic
  contrib-libs-cxxsupp
  yutil
  yt-yt-core
  contrib-clickhouse-src
  cpp-json-yson
)

target_sources(yt-library-clickhouse_functions PRIVATE
  ${PROJECT_SOURCE_DIR}/yt/yt/library/clickhouse_functions/unescaped_yson.cpp
  ${PROJECT_SOURCE_DIR}/yt/yt/library/clickhouse_functions/yson_parser_adapter.cpp
)


add_global_library_for(yt-library-clickhouse_functions.global
  yt-library-clickhouse_functions)


target_compile_options(yt-library-clickhouse_functions.global PRIVATE
  -Wdeprecated-this-capture
  -Wimplicit-fallthrough
  -Wparentheses
  -Wno-logical-op-parentheses
  -Wno-bitwise-op-parentheses
)

target_include_directories(yt-library-clickhouse_functions.global PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/src
  ${PROJECT_SOURCE_DIR}/contrib/libs/double-conversion
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/Foundation/include
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/Net/include
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/Util/include
  ${PROJECT_SOURCE_DIR}/contrib/clickhouse/base/poco/XML/include
  ${PROJECT_SOURCE_DIR}/contrib/libs/re2
  ${PROJECT_SOURCE_DIR}/contrib/libs/rapidjson/include
)

target_link_libraries(yt-library-clickhouse_functions.global PUBLIC
  linux-headers-generic::linux-headers-generic
  contrib-libs-cxxsupp
  yutil
  yt-yt-core
  contrib-clickhouse-src
  cpp-json-yson
)

target_sources(yt-library-clickhouse_functions.global PRIVATE
  ${PROJECT_SOURCE_DIR}/yt/yt/library/clickhouse_functions/convert_yson.cpp
  ${PROJECT_SOURCE_DIR}/yt/yt/library/clickhouse_functions/yson_to_json.cpp
  ${PROJECT_SOURCE_DIR}/yt/yt/library/clickhouse_functions/ypath.cpp
  ${PROJECT_SOURCE_DIR}/yt/yt/library/clickhouse_functions/yson_extract.cpp
)

