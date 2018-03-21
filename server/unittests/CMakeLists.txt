include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

add_executable(unittester-ypserver
  ${YT_SRCS_RESOLVED}
)

target_link_libraries(unittester-ypserver
  yt-core-test-framework
  yp-library-client
  yp-library-server
)
