SET(PROTO_NAMESPACE yt/${YT_ABI})

include(Sources.txt)

set(YT_SOURCE_FILES)
protoc(${CMAKE_SOURCE_DIR}/mapreduce/yt/interface/protos/extension.proto YT_SOURCE_FILES)
include(query_client/CMakeLists.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)
list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

add_library(yt-library-client ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-client
  deps-arcadia-contrib-libs-re2
  deps-arcadia-library-http
  yt-core
  yt-core-codegen
  yt-core-crypto
)
