include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core-https ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core-https
  yt-core
  yt-core-rpc-grpc
  yt-core-http
  yt-core-crypto
)
