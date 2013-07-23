set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/jerasure )
set( SRCS
  ${BASE}/cauchy_best_r6.c
  ${BASE}/galois.c
  ${BASE}/jerasure.c
  ${BASE}/liberation.c
  ${BASE}/reed_sol.c
)

set_source_files_properties(
  ${SRCS}
  PROPERTIES LANGUAGE CXX
)

add_library( ytext-jerasure STATIC ${SRCS} )

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties( ytext-jerasure PROPERTIES
  VERSION   1.2
  SOVERSION 0.1
)

