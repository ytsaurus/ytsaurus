include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
)

add_library(yt-scheduler-simulator ${YT_SRCS_RESOLVED})

target_link_libraries(yt-scheduler-simulator
  yt-library-ytlib
  yt-library-server
  yt-core-alloc
)

add_executable(scheduler_simulator
  bin/simulator/main.cpp
)

add_executable(convert_operations_to_binary_format
  bin/converter/convert.cpp
)

target_link_libraries(scheduler_simulator
  yt-scheduler-simulator
)

target_link_libraries(convert_operations_to_binary_format
  yt-scheduler-simulator
)
