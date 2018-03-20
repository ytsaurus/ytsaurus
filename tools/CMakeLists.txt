SET(DIRS
  scheduler_simulator
)

foreach(dir ${DIRS})
  add_subdirectory(${dir})
endforeach()
