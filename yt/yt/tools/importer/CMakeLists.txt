
# This file was generated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.

add_executable(importer)


target_link_libraries(importer PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-mapreduce-client
  cpp-mapreduce-util
  mapreduce-library-blob_table
  yt-library-arrow_parquet_adapter
  yt-library-huggingface_client
  yt-library-s3
  cpp-yson-node
  library-cpp-getopt
  libs-apache-arrow
)

target_link_options(importer PRIVATE
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

target_sources(importer PRIVATE
  ${PROJECT_SOURCE_DIR}/yt/yt/tools/importer/main.cpp
)



target_allocator(importer
  cpp-malloc-tcmalloc
  libs-tcmalloc-no_percpu_cache
)

vcs_info(importer)




