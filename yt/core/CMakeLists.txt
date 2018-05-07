add_subdirectory(codegen)
add_subdirectory(rpc/grpc)
add_subdirectory(rpc/http)
add_subdirectory(http)
add_subdirectory(crypto)
add_subdirectory(re2)
add_subdirectory(skiff)

include(Sources.txt)

if(YT_ADD_HEADERS_TO_SRCS)
    file(GLOB_RECURSE HEADERS ${CMAKE_CURRENT_LIST_DIR}/*.h)
    set(YT_SRCS ${YT_SRCS} ${HEADERS})
endif()

list(APPEND YT_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/execution_context-supp.S
)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core
  deps-arcadia-contrib-libs-c--ares
  deps-arcadia-contrib-libs-farmhash
  deps-arcadia-contrib-libs-libbz2
  deps-arcadia-contrib-libs-lz4
  deps-arcadia-contrib-libs-lzmasdk
  deps-arcadia-contrib-libs-protobuf
  deps-arcadia-contrib-libs-snappy
  deps-arcadia-contrib-libs-zstd
  deps-arcadia-contrib-libs-yajl
  deps-arcadia-library-streams-lz
  deps-arcadia-library-streams-brotli
  deps-arcadia-library-getopt
  deps-arcadia-util
  deps-yt-contrib-quicklz
  deps-yt-contrib-zstd-legacy
  deps-yt-contrib-http-parser
  yt-build
  yt-contrib-jerasure
  deps-arcadia-contrib-libs-zlib # duplicate zlib dependency here, because LLVM depends on zlib
)

if(CMAKE_COMPILER_IS_CLANG AND NOT APPLE)
  target_link_libraries(yt-core -latomic -ldl)
endif()

if(NOT APPLE)
  target_link_libraries(yt-core -lutil)
endif()
