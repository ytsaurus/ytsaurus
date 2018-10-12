include(Sources.txt)

set(YT_SRCS_RESOLVED)
resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-server-misc
  ${YT_SRCS_RESOLVED}
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp)

target_link_libraries(yt-library-server-misc
  yt-library-ytlib)
