set( BASE ${CMAKE_SOURCE_DIR}/contrib/yajl/src )
set( SRCS
  ${BASE}/yajl.c
  ${BASE}/yajl_alloc.c
  ${BASE}/yajl_buf.c
  ${BASE}/yajl_encode.c
  ${BASE}/yajl_gen.c
  ${BASE}/yajl_lex.c
  ${BASE}/yajl_parser.c
  ${BASE}/yajl_tree.c
  ${BASE}/yajl_version.c
)

add_library( ytext-yajl STATIC ${SRCS} )

# Compile properties
SET(CMAKE_C_FLAGS
  "${CMAKE_C_FLAGS} -std=c99 -pedantic -Wpointer-arith -Wno-format-y2k -Wstrict-prototypes -Wmissing-declarations -Wnested-externs -Wextra  -Wundef -Wwrite-strings -Wold-style-definition -Wredundant-decls -Wno-unused-parameter -Wno-sign-compare -Wmissing-prototypes")

set_source_files_properties(
  ${SRCS}
  PROPERTIES LANGUAGE C
)

# Version
SET (YAJL_MAJOR 2)
SET (YAJL_MINOR 0)
SET (YAJL_MICRO 5)

set_target_properties( ytext-yajl PROPERTIES
  VERSION ${YAJL_MAJOR}.${YAJL_MINOR}.${YAJL_MICRO}
  SOVERSION 0.1
)

# Prepare headers
SET (PUB_HDRS api/yajl_parse.h api/yajl_gen.h api/yajl_common.h api/yajl_tree.h)
SET (INC_DIR ${CMAKE_BINARY_DIR}/include/yajl)
FILE(MAKE_DIRECTORY ${INC_DIR})
CONFIGURE_FILE(${BASE}/api/yajl_version.h.cmake ${INC_DIR}/yajl_version.h)

FOREACH (header ${PUB_HDRS})
  SET (header ${CMAKE_CURRENT_SOURCE_DIR}/${header})

  EXEC_PROGRAM(${CMAKE_COMMAND} ARGS -E copy_if_different ${header} ${INC_DIR})

  ADD_CUSTOM_COMMAND(TARGET yajl_s POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different ${header} ${INC_DIR})
ENDFOREACH (header ${PUB_HDRS})

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${INC_DIR}/..
)


