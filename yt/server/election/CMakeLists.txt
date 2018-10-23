include(Sources.txt)

set(YT_SRCS_RESOLVED)
resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-server-election ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-server-election
  yt-library-ytlib
)
