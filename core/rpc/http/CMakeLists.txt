include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core-rpc-http ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core-rpc-http
  yt-core
  yt-core-http
)
