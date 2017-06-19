include(Sources.txt)

list(APPEND YT_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/execution_context-supp.S
)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  list(APPEND YT_SRCS
    ${CMAKE_CURRENT_LIST_DIR}/containers/container_manager.cpp
    ${CMAKE_CURRENT_LIST_DIR}/containers/instance.cpp
    ${CMAKE_CURRENT_LIST_DIR}/containers/porto_executor.cpp
    ${CMAKE_CURRENT_LIST_DIR}/containers/private.cpp
  )
endif()

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core ${YT_SRCS_RESOLVED})

llvm_map_components_to_libraries(
  REQUIRED_LLVM_LIBRARIES
  mcjit ipo x86 mcdisassembler linker asmparser irreader instrumentation
)

# We link statically with zlib, and LLVM (sometimes) tries to bring its own
# dependency.
list(REMOVE_ITEM REQUIRED_LLVM_LIBRARIES "-lz")

target_link_libraries(yt-core
  deps-arcadia-contrib-libs-brotli
  deps-arcadia-contrib-libs-c--ares
  deps-arcadia-contrib-libs-libbz2
  deps-arcadia-contrib-libs-lzmasdk
  deps-arcadia-contrib-libs-protobuf
  deps-arcadia-contrib-libs-snappy
  deps-arcadia-contrib-libs-zstd07
  deps-arcadia-library-http
  deps-arcadia-library-streams-lz
  deps-arcadia-library-getopt
  deps-arcadia-util
  deps-yt-contrib-farmhash
  deps-yt-contrib-quicklz
  deps-yt-contrib-zstd-legacy
  yt-build
  yt-contrib-jerasure
  yt-contrib-libev
  ${REQUIRED_LLVM_LIBRARIES}
  deps-arcadia-contrib-libs-zlib # duplicate zlib dependency here, because LLVM depends on zlib
)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  target_link_libraries(yt-core deps-yt-contrib-coredumper)
  target_link_libraries(yt-core deps-yt-contrib-portoapi)
endif()

if(CMAKE_COMPILER_IS_CLANG AND NOT APPLE)
  target_link_libraries(yt-core -latomic -ldl)
endif()

if(NOT APPLE)
  target_link_libraries(yt-core -lutil)
endif()

