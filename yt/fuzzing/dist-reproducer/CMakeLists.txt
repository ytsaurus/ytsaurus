add_executable(dist-reproducer)

target_link_libraries(dist-reproducer PUBLIC
  contrib-libs-protobuf-mutator
  yutil
  yt-server-master
  yt-server-node
  yt-server-rpc_proxy
)
target_link_options(dist-reproducer PRIVATE
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
target_sources(dist-reproducer PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/fuzzing/dist-reproducer/main.cpp
)

target_allocator(dist-reproducer
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(dist-reproducer)
