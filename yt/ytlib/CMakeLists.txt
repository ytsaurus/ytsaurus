SET(PROTO_NAMESPACE yt/${YT_ABI})

include(Sources.txt)

add_subdirectory(auth)

if(YT_ADD_HEADERS_TO_SRCS)
    file(GLOB_RECURSE HEADERS ${CMAKE_CURRENT_LIST_DIR}/*.h)
    set(YT_SRCS ${YT_SRCS} ${HEADERS})
endif()

set(YT_SOURCE_FILES)
protoc(${CMAKE_SOURCE_DIR}/mapreduce/yt/interface/protos/extension.proto YT_SOURCE_FILES)
include(query_client/CMakeLists.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)
list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

add_library(yt-library-ytlib ${YT_SRCS_RESOLVED})

# These protos are used in YT python client.

SET(PROTO_TARGETS "")

target_link_libraries(yt-library-ytlib
  deps-arcadia-contrib-libs-re2
  deps-arcadia-library-http
  yt-core
  yt-core-http
  yt-core-codegen
  yt-core-crypto
  yt-core-skiff
  yt-core-erasure
  yt-library-client
)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    target_link_libraries(yt-library-ytlib deps-yt-contrib-coredumper)
    target_link_libraries(yt-library-ytlib deps-yt-contrib-portoapi)
endif()
