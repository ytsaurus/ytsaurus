add_library(contrib-libs-protobuf-mutator)
target_include_directories(contrib-libs-protobuf-mutator PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src
)
target_link_libraries(contrib-libs-protobuf-mutator PUBLIC
  contrib-libs-protobuf
)
target_sources(contrib-libs-protobuf-mutator PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/binary_format.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/mutator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/text_format.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/utf8_fix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_mutator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/port/protobuf.h
)

add_global_library_for(contrib-libs-protobuf-mutator.global contrib-libs-protobuf-mutator)
target_include_directories(contrib-libs-protobuf-mutator.global PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src
)
target_link_libraries(contrib-libs-protobuf-mutator.global PUBLIC
  contrib-libs-protobuf
)
target_sources(contrib-libs-protobuf-mutator.global PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/binary_format.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/mutator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/text_format.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/utf8_fix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_mutator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf-mutator/port/protobuf.h
)
