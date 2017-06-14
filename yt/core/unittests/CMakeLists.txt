SET(PROTO_NAMESPACE yt/${YT_ABI})

include(Sources.txt)

set(YT_SRCS_RESOLVED)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_executable(unittester-core
  ${YT_SRCS_RESOLVED}
  ${YT_UNITTESTS_MAIN}
)

target_include_directories(unittester-core
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(unittester-core
  yt-core
  yt-core-test-framework
)
