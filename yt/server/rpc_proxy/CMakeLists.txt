include(Sources.txt)

set(YT_SRCS_RESOLVED)
resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-server-rpc-proxy ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-server-rpc-proxy
  yt-core-https
  yt-library-ytlib
  yt-library-ytlib-auth
  yt-library-server-misc
)
