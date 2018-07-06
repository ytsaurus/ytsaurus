include(Sources.txt)

resolve_srcs(YP_SRCS YP_SRCS_RESOLVED)

add_library(yp-library-client ${YP_SRCS_RESOLVED})

# These protos are used in YP python client.

set(PROTO_TARGETS "")

foreach(proto_file "object_service.proto" "data_model.proto" "discovery_service.proto"
                   "cluster_api.proto" "pod_agent.proto")
  prepare_proto(${PROJECT_SOURCE_DIR}/yp/python yp/client/api/proto/${proto_file} PROTO_TARGETS)
endforeach(proto_file)

add_custom_target(yp-python-protos ALL DEPENDS ${PROTO_TARGETS})

add_dependencies(yp-python-protos yt-core-python-protos)

target_link_libraries(yp-library-client
  yt-core
  yp-build
)
