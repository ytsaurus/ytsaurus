set(NAME yt-contrib-jerasure)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/jerasure)
set(SRCS
  ${BASE}/cauchy_best_r6.c
  ${BASE}/galois.c
  ${BASE}/jerasure.c
  ${BASE}/liberation.c
  ${BASE}/reed_sol.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

set_target_properties(${NAME} PROPERTIES FOLDER "contrib")

if(CMAKE_COMPILER_IS_GNUCXX)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -Wno-unused-variable)
endif()

