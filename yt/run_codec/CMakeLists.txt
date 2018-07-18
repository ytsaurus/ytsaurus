add_executable(run_codec
  main.cpp
)

target_link_libraries(run_codec
  yt-library-ytlib
  deps-arcadia-library-lfalloc
)
