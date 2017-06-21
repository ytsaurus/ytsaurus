set(NAME deps-arcadia-contrib-libs-protobuf)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf)
set(GRPC_BASE ${CMAKE_SOURCE_DIR}/contrib/libs/grpc)

set(SRCS_PROTOBUF
  ${BASE}/stubs/atomicops_internals_x86_gcc.cc
  ${BASE}/stubs/bytestream.cc
  ${BASE}/stubs/common.cc
  ${BASE}/stubs/int128.cc
  ${BASE}/stubs/mathlimits.cc
  ${BASE}/stubs/once.cc
  ${BASE}/stubs/status.cc
  ${BASE}/stubs/statusor.cc
  ${BASE}/stubs/stringpiece.cc
  ${BASE}/stubs/stringprintf.cc
  ${BASE}/stubs/structurally_valid.cc
  ${BASE}/stubs/strutil.cc
  ${BASE}/stubs/substitute.cc
  ${BASE}/stubs/time.cc

  ${BASE}/io/coded_stream.cc
  ${BASE}/io/gzip_stream.cc
  ${BASE}/io/printer.cc
  ${BASE}/io/strtod.cc
  ${BASE}/io/tokenizer.cc
  ${BASE}/io/zero_copy_stream.cc
  ${BASE}/io/zero_copy_stream_impl.cc
  ${BASE}/io/zero_copy_stream_impl_lite.cc

  ${BASE}/any.cc
  ${BASE}/any.pb.cc
  ${BASE}/api.pb.cc
  ${BASE}/arena.cc
  ${BASE}/arenastring.cc
  ${BASE}/descriptor.cc
  ${BASE}/descriptor_database.cc
  ${BASE}/descriptor.pb.cc
  ${BASE}/duration.pb.cc
  ${BASE}/dynamic_message.cc
  ${BASE}/empty.pb.cc
  ${BASE}/extension_set.cc
  ${BASE}/extension_set_heavy.cc
  ${BASE}/field_mask.pb.cc
  ${BASE}/generated_message_reflection.cc
  ${BASE}/generated_message_util.cc
  ${BASE}/json_util.cc
  ${BASE}/map_field.cc
  ${BASE}/message.cc
  ${BASE}/message_lite.cc
  ${BASE}/messagext.cc
  ${BASE}/messagext_lite.cc
  ${BASE}/reflection_ops.cc
  ${BASE}/repeated_field.cc
  ${BASE}/service.cc
  ${BASE}/source_context.pb.cc
  ${BASE}/struct.pb.cc
  ${BASE}/text_format.cc
  ${BASE}/timestamp.pb.cc
  ${BASE}/type.pb.cc
  ${BASE}/unknown_field_set.cc
  ${BASE}/wire_format.cc
  ${BASE}/wire_format_lite.cc
  ${BASE}/wrappers.pb.cc
)

set(SRCS_PROTOC
  ${BASE}/compiler/code_generator.cc
  ${BASE}/compiler/command_line_interface.cc
  ${BASE}/compiler/importer.cc
  ${BASE}/compiler/parser.cc
  ${BASE}/compiler/plugin.cc
  ${BASE}/compiler/plugin.pb.cc
  ${BASE}/compiler/subprocess.cc
  ${BASE}/compiler/zip_writer.cc

  ${BASE}/compiler/cpp/cpp_enum.cc
  ${BASE}/compiler/cpp/cpp_enum_field.cc
  ${BASE}/compiler/cpp/cpp_extension.cc
  ${BASE}/compiler/cpp/cpp_field.cc
  ${BASE}/compiler/cpp/cpp_file.cc
  ${BASE}/compiler/cpp/cpp_generator.cc
  ${BASE}/compiler/cpp/cpp_helpers.cc
  ${BASE}/compiler/cpp/cpp_map_field.cc
  ${BASE}/compiler/cpp/cpp_message.cc
  ${BASE}/compiler/cpp/cpp_message_field.cc
  ${BASE}/compiler/cpp/cpp_primitive_field.cc
  ${BASE}/compiler/cpp/cpp_service.cc
  ${BASE}/compiler/cpp/cpp_string_field.cc

  ${BASE}/compiler/java/java_context.cc
  ${BASE}/compiler/java/java_doc_comment.cc
  ${BASE}/compiler/java/java_enum.cc
  ${BASE}/compiler/java/java_enum_lite.cc
  ${BASE}/compiler/java/java_enum_field.cc
  ${BASE}/compiler/java/java_enum_field_lite.cc
  ${BASE}/compiler/java/java_extension.cc
  ${BASE}/compiler/java/java_extension_lite.cc
  ${BASE}/compiler/java/java_field.cc
  ${BASE}/compiler/java/java_file.cc
  ${BASE}/compiler/java/java_generator.cc
  ${BASE}/compiler/java/java_generator_factory.cc
  ${BASE}/compiler/java/java_helpers.cc
  ${BASE}/compiler/java/java_lazy_message_field.cc
  ${BASE}/compiler/java/java_lazy_message_field_lite.cc
  ${BASE}/compiler/java/java_map_field.cc
  ${BASE}/compiler/java/java_map_field_lite.cc
  ${BASE}/compiler/java/java_message_builder.cc
  ${BASE}/compiler/java/java_message_builder_lite.cc
  ${BASE}/compiler/java/java_message.cc
  ${BASE}/compiler/java/java_message_lite.cc
  ${BASE}/compiler/java/java_message_field.cc
  ${BASE}/compiler/java/java_message_field_lite.cc
  ${BASE}/compiler/java/java_name_resolver.cc
  ${BASE}/compiler/java/java_primitive_field.cc
  ${BASE}/compiler/java/java_primitive_field_lite.cc
  ${BASE}/compiler/java/java_service.cc
  ${BASE}/compiler/java/java_shared_code_generator.cc
  ${BASE}/compiler/java/java_string_field.cc
  ${BASE}/compiler/java/java_string_field_lite.cc

  ${BASE}/compiler/python/python_generator.cc

  ${BASE}/compiler/perlxs/perlxs_generator.cc
  ${BASE}/compiler/perlxs/perlxs_helpers.cc
)

SET(PROTO_MAIN
  ${BASE}/compiler/main.cc
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${BASE}
  #${BASE}/io
  ${BASE}/google/protobuf
)

add_library(${NAME} STATIC ${SRCS_PROTOBUF})

target_link_libraries(${NAME} deps-arcadia-util)

add_library(${NAME}-protoc STATIC ${SRCS_PROTOC})

target_link_libraries(${NAME}-protoc ${NAME})

add_executable(protoc ${PROTO_MAIN})

target_link_libraries(protoc ${NAME}-protoc deps-arcadia-util -ldl)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${BASE}/python/google
  ${CMAKE_SOURCE_DIR}/python/google
)

