include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-scheduler-simulator ${YT_SRCS_RESOLVED})

target_link_libraries(yt-scheduler-simulator
  yt-library-client
  yt-library-server
)

add_executable(scheduler_simulator
  main.cpp
  yt-scheduler-simulator
)

add_executable(convert_operations_to_binary_format
  convert.cpp
  yt-scheduler-simulator
)

