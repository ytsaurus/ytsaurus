include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core-re2 ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core-re2
  deps-arcadia-contrib-libs-re2
  yt-core
)
