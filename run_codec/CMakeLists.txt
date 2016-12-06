add_executable(run_codec
  main.cpp
)

target_link_libraries(run_codec
  yt-library-client
  deps-arcadia-library-lfalloc
)
