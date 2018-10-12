SET(DIRS
  run_codec
  scheduler_simulator
)

foreach(dir ${DIRS})
  add_subdirectory(${dir})
endforeach()
