include(Sources.txt)

resolve_srcs(YP_SRCS YP_SRCS_RESOLVED)

add_library(yp-library-client ${YP_SRCS_RESOLVED})

target_link_libraries(yp-library-client
  yt-core
  yp-build
)