
# This file was generated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.

get_built_tool_path(
  TOOL_protoc_bin
  TOOL_protoc_dependency
  contrib/tools/protoc/bin
  protoc
)

get_built_tool_path(
  TOOL_cpp_styleguide_bin
  TOOL_cpp_styleguide_dependency
  contrib/tools/protoc/plugins/cpp_styleguide
  cpp_styleguide
)

add_library(library-ytprof-proto)


target_include_directories(library-ytprof-proto PUBLIC
  ${PROJECT_BINARY_DIR}/yt
)

target_link_libraries(library-ytprof-proto PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)

target_proto_messages(library-ytprof-proto PRIVATE
  ${PROJECT_SOURCE_DIR}/yt/yt/library/ytprof/proto/profile.proto
)

target_sources(library-ytprof-proto PRIVATE
  ${PROJECT_BINARY_DIR}/yt/yt/library/ytprof/proto/profile.pb.h
)



target_proto_addincls(library-ytprof-proto
  ./yt
  ${PROJECT_SOURCE_DIR}/yt
  ${PROJECT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/yt
  ${PROJECT_SOURCE_DIR}/contrib/libs/protobuf/src
  ${PROJECT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/contrib/libs/protobuf/src
)

target_proto_outs(library-ytprof-proto
  --cpp_out=${PROJECT_BINARY_DIR}/yt
  --cpp_styleguide_out=${PROJECT_BINARY_DIR}/yt
)




