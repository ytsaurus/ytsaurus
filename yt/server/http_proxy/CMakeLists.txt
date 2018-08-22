include(Sources.txt)

set(YT_SRCS_RESOLVED)
resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-server-http-proxy ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-server-http-proxy
  yt-core-https
  yt-library-server)
