set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf )

set( SRCS_PROTOBUF
  ${BASE}/stubs/common.cc
  ${BASE}/stubs/once.cc
  ${BASE}/stubs/structurally_valid.cc
  ${BASE}/stubs/substitute.cc
  ${BASE}/stubs/strutil.cc
  ${BASE}/stubs/stringprintf.cc
  ${BASE}/stubs/atomicops_internals_x86_gcc.cc

  ${BASE}/io/coded_stream.cc
  ${BASE}/io/printer.cc
  ${BASE}/io/tokenizer.cc
  ${BASE}/io/zero_copy_stream.cc
  ${BASE}/io/zero_copy_stream_impl.cc
  ${BASE}/io/zero_copy_stream_impl_lite.cc
  ${BASE}/io/strtod.cc

  ${BASE}/compiler/importer.cc
  ${BASE}/compiler/parser.cc

  ${BASE}/descriptor.cc
  ${BASE}/descriptor.pb.cc
  ${BASE}/descriptor_database.cc
  ${BASE}/dynamic_message.cc
  ${BASE}/extension_set.cc
  ${BASE}/extension_set_heavy.cc
  ${BASE}/generated_message_reflection.cc
  ${BASE}/generated_message_util.cc
  ${BASE}/message.cc
  ${BASE}/message_lite.cc
  ${BASE}/messagext.cc
  ${BASE}/reflection_ops.cc
  ${BASE}/repeated_field.cc
  ${BASE}/service.cc
  ${BASE}/text_format.cc
  ${BASE}/unknown_field_set.cc
  ${BASE}/wire_format.cc
  ${BASE}/wire_format_lite.cc
)

set( SRCS_PROTOC
  ${BASE}/compiler/main.cc

  ${BASE}/compiler/code_generator.cc
  ${BASE}/compiler/command_line_interface.cc
  ${BASE}/compiler/subprocess.cc
  ${BASE}/compiler/zip_writer.cc
  ${BASE}/compiler/plugin.cc
  ${BASE}/compiler/plugin.pb.cc

  ${BASE}/compiler/cpp/cpp_enum.cc
  ${BASE}/compiler/cpp/cpp_enum_field.cc
  ${BASE}/compiler/cpp/cpp_extension.cc
  ${BASE}/compiler/cpp/cpp_field.cc
  ${BASE}/compiler/cpp/cpp_file.cc
  ${BASE}/compiler/cpp/cpp_generator.cc
  ${BASE}/compiler/cpp/cpp_helpers.cc
  ${BASE}/compiler/cpp/cpp_message.cc
  ${BASE}/compiler/cpp/cpp_message_field.cc
  ${BASE}/compiler/cpp/cpp_primitive_field.cc
  ${BASE}/compiler/cpp/cpp_service.cc
  ${BASE}/compiler/cpp/cpp_string_field.cc

  ${BASE}/compiler/java/java_enum.cc
  ${BASE}/compiler/java/java_enum_field.cc
  ${BASE}/compiler/java/java_extension.cc
  ${BASE}/compiler/java/java_field.cc
  ${BASE}/compiler/java/java_file.cc
  ${BASE}/compiler/java/java_generator.cc
  ${BASE}/compiler/java/java_helpers.cc
  ${BASE}/compiler/java/java_message.cc
  ${BASE}/compiler/java/java_message_field.cc
  ${BASE}/compiler/java/java_primitive_field.cc
  ${BASE}/compiler/java/java_service.cc
  ${BASE}/compiler/java/java_context.cc
  ${BASE}/compiler/java/java_doc_comment.cc
  ${BASE}/compiler/java/java_enum.cc
  ${BASE}/compiler/java/java_enum_field.cc
  ${BASE}/compiler/java/java_extension.cc
  ${BASE}/compiler/java/java_field.cc
  ${BASE}/compiler/java/java_file.cc
  ${BASE}/compiler/java/java_generator.cc
  ${BASE}/compiler/java/java_generator_factory.cc
  ${BASE}/compiler/java/java_helpers.cc
  ${BASE}/compiler/java/java_lazy_message_field.cc
  ${BASE}/compiler/java/java_message.cc
  ${BASE}/compiler/java/java_message_field.cc
  ${BASE}/compiler/java/java_name_resolver.cc
  ${BASE}/compiler/java/java_primitive_field.cc
  ${BASE}/compiler/java/java_service.cc
  ${BASE}/compiler/java/java_shared_code_generator.cc
  ${BASE}/compiler/java/java_string_field.cc

  ${BASE}/compiler/python/python_generator.cc

  ${BASE}/compiler/perlxs/perlxs_generator.cc
  ${BASE}/compiler/perlxs/perlxs_helpers.cc
)

add_library( protobuf STATIC
  ${SRCS_PROTOBUF}
)

target_link_libraries( protobuf
  ytext-arcadia-util
)

add_executable( protoc
  ${SRCS_PROTOC}
)

include_directories(
  ${BASE}
  ${BASE}/io
  ${CMAKE_SOURCE_DIR}
)

target_link_libraries( protoc
  protobuf
)

if (MSVC)
  target_link_libraries( protoc ytext-zlib )
endif()
