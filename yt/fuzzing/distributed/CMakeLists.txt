add_executable(fuzztests-distributed)

if (ENABLE_COVERAGE)
  target_compile_options(fuzztests-distributed PRIVATE -fsanitize=fuzzer -fprofile-instr-generate -fcoverage-mapping)
  target_link_options(fuzztests-distributed PRIVATE -fsanitize=fuzzer -fprofile-instr-generate -fcoverage-mapping)
else()
  target_compile_options(fuzztests-distributed PRIVATE -fsanitize=fuzzer)
  target_link_options(fuzztests-distributed PRIVATE -fsanitize=fuzzer)
endif()

target_link_libraries(fuzztests-distributed PUBLIC
  contrib-libs-protobuf-mutator
  yutil
  yt-server-master
  yt-server-node
  yt-server-rpc_proxy
)
target_link_options(fuzztests-distributed PRIVATE
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
target_sources(fuzztests-distributed PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/fuzzing/distributed/fuzz-distributed.cpp
)
target_allocator(fuzztests-distributed
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(fuzztests-distributed)
