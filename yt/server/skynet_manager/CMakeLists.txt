include(Sources.txt)

set(YT_SRCS_RESOLVED)
resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-server-skynet-manager ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-server-skynet-manager
  yt-library-ytlib
  deps-arcadia-contrib-libs-msgpack)
