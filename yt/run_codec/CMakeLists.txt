add_executable(run_codec
  main.cpp
)

target_link_libraries(run_codec
  ytlib
  ytext-arcadia-library-lfalloc
)
