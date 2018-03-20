add_executable(scheduler_simulator
  main.cpp
  operation_description.cpp
  operation.cpp
  operation_controller.cpp
)

add_executable(convert_operations_to_binary_format
  convert.cpp
  operation_description.cpp
  operation.cpp
  operation_controller.cpp
)

target_link_libraries(scheduler_simulator
  yt-library-client
  yt-library-server
)

target_link_libraries(convert_operations_to_binary_format
  yt-library-client
  yt-library-server
)
