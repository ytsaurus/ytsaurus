add_executable(fuzztests-datanode)

if (ENABLE_COVERAGE)
  target_compile_options(fuzztests-datanode PRIVATE -fsanitize=fuzzer -fprofile-instr-generate -fcoverage-mapping)
  target_link_options(fuzztests-datanode PRIVATE -fsanitize=fuzzer -fprofile-instr-generate -fcoverage-mapping)
else()
  target_compile_options(fuzztests-datanode PRIVATE -fsanitize=fuzzer)
  target_link_options(fuzztests-datanode PRIVATE -fsanitize=fuzzer)
endif()

target_link_libraries(fuzztests-datanode PUBLIC
  contrib-libs-protobuf-mutator
  yutil
  yt-server-master
  yt-server-node
)
target_link_options(fuzztests-datanode PRIVATE
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -fPIC
  -lpthread
  -lrt
  -ldl
  -lutil
)
target_sources(fuzztests-datanode PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/fuzzing/datanode/fuzz-datanode.cpp
)
target_allocator(fuzztests-datanode
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(fuzztests-datanode)
