include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

add_executable(unittester-skynet-manager
  ${YT_SRCS_RESOLVED}
)

target_link_libraries(unittester-skynet-manager
  yt-core-test-framework
  yt-library-ytlib
  yt-library-server-skynet-manager
)
