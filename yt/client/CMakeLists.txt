SET(PROTO_NAMESPACE yt/${YT_ABI})

include(Sources.txt)

if(YT_ADD_HEADERS_TO_SRCS)
    file(GLOB_RECURSE HEADERS ${CMAKE_CURRENT_LIST_DIR}/*.h)
    set(YT_SRCS ${YT_SRCS} ${HEADERS})
endif()

set(YT_SOURCE_FILES)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)
list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/api/rpc_proxy/protocol_version_variables.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/api/rpc_proxy/protocol_version_variables.h
  @ONLY
)

foreach(proto_suffix "client/hive/proto/timestamp_map.proto"
                     "client/rpc_proxy/proto/api_service.proto"
                     "client/rpc_proxy/proto/discovery_service.proto")
  prepare_proto(${PROJECT_SOURCE_DIR}/python yt/${proto_suffix} PROTO_TARGETS)
endforeach(proto_suffix)


add_library(yt-library-client ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-client
  deps-arcadia-contrib-libs-re2
  deps-arcadia-library-http
  yt-core
  yt-core-http
)
