include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-ytlib-auth ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-ytlib-auth
  yt-library-ytlib
)
