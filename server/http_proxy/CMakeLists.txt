include(Sources.txt)

set(YT_SRCS_RESOLVED)
resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-server-http-proxy ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-server-http-proxy
  yt-core-https
  yt-core-alloc
  yt-core-phdr-cache
  yt-library-ytlib
  yt-library-ytlib-auth
  yt-library-server-misc
  deps-arcadia-library-string_utils-base64
  deps-arcadia-library-streams-lzop
)
