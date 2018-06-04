include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-client-auth ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-client-auth
  yt-library-client
)
