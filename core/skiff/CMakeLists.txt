include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core-skiff ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core-skiff
  yt-core
)
