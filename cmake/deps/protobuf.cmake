set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf )

set( SRCS_PROTOBUF
  ${BASE}/stubs/common.cc
  ${BASE}/stubs/hash.cc
  ${BASE}/stubs/map-util.cc
  ${BASE}/stubs/once.cc
  ${BASE}/stubs/structurally_valid.cc
  ${BASE}/stubs/stl_util-inl.cc
  ${BASE}/stubs/substitute.cc
  ${BASE}/stubs/strutil.cc

  ${BASE}/io/coded_stream.cc
  ${BASE}/io/printer.cc
  ${BASE}/io/tokenizer.cc
  ${BASE}/io/zero_copy_stream.cc
  ${BASE}/io/zero_copy_stream_impl.cc
  ${BASE}/io/zero_copy_stream_impl_lite.cc

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

  ${BASE}/compiler/python/python_generator.cc
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

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( protoc stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( protoc PROPERTIES LINK_FLAGS "-nodefaultlibs" )
  endif()
endif()

if (MSVC)
  if (YT_BUILD_WITH_STLPORT)
    set_target_properties( protoc PROPERTIES
      LINK_FLAGS "/LIBPATH:${CMAKE_BINARY_DIR}/bin"
    )
  endif()
  target_link_libraries( protoc ytext-zlib )
endif()
