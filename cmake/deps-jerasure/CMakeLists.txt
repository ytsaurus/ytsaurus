set( LTYPE STATIC )

set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/jerasure )

IF(CMAKE_COMPILER_IS_GNUCXX)
  set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable" )
ENDIF(CMAKE_COMPILER_IS_GNUCXX)

set(SRC
  ${BASE}/cauchy.c
  ${BASE}/cauchy_best_r6.c
  ${BASE}/galois.c
  ${BASE}/jerasure.c
  ${BASE}/liberation.c
  ${BASE}/reed_sol.c
)

SET_SOURCE_FILES_PROPERTIES( ${SRC} PROPERTIES LANGUAGE CXX )

add_library( ytext-jerasure ${LTYPE} ${SRC})

include_directories(
  ${BASE}
  ${CMAKE_SOURCE_DIR}
)

set_target_properties( ytext-jerasure PROPERTIES
  VERSION   1.2
  SOVERSION 0.1
)

install(
  TARGETS
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)

