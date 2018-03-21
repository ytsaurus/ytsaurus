function(PUT_PB2 proto targets)
  get_filename_component(_proto_dirname  ${proto} PATH)
  get_filename_component(_proto_basename ${proto} NAME_WE)

  set(OUTPUT_PATH "${CMAKE_CURRENT_LIST_DIR}/proto/${_proto_dirname}/${_proto_basename}")
  set(OUTPUT "${OUTPUT_PATH}_pb2.py" "${OUTPUT_PATH}_pb2_grpc.py")

  add_custom_command(
    OUTPUT
      ${OUTPUT}
    COMMAND
      ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/prepare_proto.py ${CMAKE_BINARY_DIR} ${_proto_dirname}/${_proto_basename}
    DEPENDS
      yp-library-client
      yt-core
      ${${targets}}  # fictive target to ensure sequential build
    WORKING_DIRECTORY
      ${CMAKE_CURRENT_LIST_DIR}
    COMMENT "Preparing pb2 modules for ${proto}..."
  )

  set(${targets} ${${targets}} ${OUTPUT} PARENT_SCOPE)
endfunction()

set(TARGETS "")

put_pb2(yt/core/misc/proto/error.proto TARGETS)
put_pb2(yt/core/ytree/proto/attributes.proto TARGETS)
put_pb2(yt/core/yson/proto/protobuf_interop.proto TARGETS)
put_pb2(yp/client/api/proto/object_service.proto TARGETS)
put_pb2(yp/client/api/proto/data_model.proto TARGETS)
put_pb2(yp/client/api/proto/discovery_service.proto TARGETS)
put_pb2(yp/client/api/proto/cluster_api.proto TARGETS)

add_custom_target(
    yp-python-protos
  ALL
  DEPENDS
    ${TARGETS}
  WORKING_DIRECTORY
    ${CMAKE_CURRENT_LIST_DIR}
)

foreach(package_name "prettytable")
  file(
    REMOVE_RECURSE
    ${CMAKE_CURRENT_SOURCE_DIR}/yp/packages/${package_name}*
    ${CMAKE_CURRENT_SOURCE_DIR}/yp/packages/${package_name}*.py
  )

  SET(PACKAGE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../contrib/python/${package_name})

  file(GLOB FILES_TO_COPY
    ${PACKAGE_DIR}/${package_name}*.py
  )

  SET(SOURCE_DIR_INSIDE_PACKAGE ${PACKAGE_DIR}/${package_name})
  if(EXISTS ${SOURCE_DIR_INSIDE_PACKAGE})
    SET(FILES_TO_COPY ${FILES_TO_COPY} ${SOURCE_DIR_INSIDE_PACKAGE})
  endif()

  file(
    COPY ${FILES_TO_COPY}
    DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yp/packages
  )
endforeach(package_name)
