include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core-rpc-grpc ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core-rpc-grpc
  yt-core
  deps-arcadia-contrib-libs-grpc-core
)
