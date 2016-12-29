set(NAME deps-yt-contrib-coredumper)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/coredumper)
set(SRCS
  ${BASE}/coredumper.cpp
  ${BASE}/elfcore.cpp
  ${BASE}/linuxthreads.cpp
  ${BASE}/thread_lister.cpp
)

set_source_files_properties(
  ${SRCS}
  PROPERTIES
  COMPILE_FLAGS "-Wno-extern-c-compat -fno-omit-frame-pointer"
)

add_library(${NAME} STATIC ${SRCS})

