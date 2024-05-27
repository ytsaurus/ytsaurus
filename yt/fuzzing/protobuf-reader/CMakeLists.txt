add_executable(protobuf-reader)

target_link_libraries(protobuf-reader PUBLIC
  contrib-libs-protobuf-mutator
  yutil
  yt-server-master
  yt-server-node
  yt-server-rpc_proxy
)
target_link_options(protobuf-reader PRIVATE
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
target_sources(protobuf-reader PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/fuzzing/protobuf-reader/main.cpp
)

target_allocator(protobuf-reader
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(protobuf-reader)
