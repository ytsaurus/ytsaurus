add_executable(reproducer)

target_link_libraries(reproducer PUBLIC
  contrib-libs-protobuf-mutator
  yutil
  yt-server-master
  yt-server-node
)
target_link_options(reproducer PRIVATE
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
target_sources(reproducer PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/fuzzing/reproducer/main.cpp
)
target_allocator(reproducer
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(reproducer)
