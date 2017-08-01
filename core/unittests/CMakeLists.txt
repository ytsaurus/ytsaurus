include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_executable(unittester-core
  ${YT_SRCS_RESOLVED}
)

target_link_libraries(unittester-core
  yt-core-test-framework
  yt-core
)
