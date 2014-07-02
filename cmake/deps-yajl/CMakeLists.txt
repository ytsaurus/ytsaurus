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
IF (WIN32)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /W4")
  ADD_DEFINITIONS(-DWIN32)
  SET(linkFlags "/PDB:NONE /INCREMENTAL:NO /OPT:NOREF /OPT:NOICF")
  SET(CMAKE_EXE_LINKER_FLAGS    "${linkFlags}"
      CACHE STRING "YAJL linker flags" FORCE)
  SET(CMAKE_EXE_LINKER_FLAGS_DEBUG ""
      CACHE STRING "YAJL debug linker flags" FORCE)
  SET(CMAKE_EXE_LINKER_FLAGS_RELEASE
      CACHE STRING "YAJL release linker flags" FORCE)
  SET(CMAKE_SHARED_LINKER_FLAGS "${linkFlags}"
      CACHE STRING "YAJL shared linker flags" FORCE)
  SET(CMAKE_MODULE_LINKER_FLAGS "${linkFlags}"
      CACHE STRING "YAJL module linker flags" FORCE)

  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4996 /wd4255 /wd4130 /wd4100 /wd4711")
  SET(CMAKE_C_FLAGS_DEBUG "/D DEBUG /Od /Z7 /MDd")
  SET(CMAKE_C_FLAGS_RELEASE "/D NDEBUG /O2")
ELSE (WIN32)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")
  IF(CMAKE_COMPILER_IS_GNUCC)
    INCLUDE(CheckCCompilerFlag)
    CHECK_C_COMPILER_FLAG(-fvisibility=hidden HAVE_GCC_VISIBILITY)
    IF(HAVE_GCC_VISIBILITY)
      SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
    ENDIF(HAVE_GCC_VISIBILITY)
  ENDIF(CMAKE_COMPILER_IS_GNUCC)
  SET(CMAKE_C_FLAGS
      "${CMAKE_C_FLAGS} -std=c99 -pedantic -Wpointer-arith -Wno-format-y2k -Wstrict-prototypes -Wmissing-declarations -Wnested-externs -Wextra  -Wundef -Wwrite-strings -Wold-style-definition -Wredundant-decls -Wno-unused-parameter -Wno-sign-compare -Wmissing-prototypes")

  SET(CMAKE_C_FLAGS_DEBUG "-DDEBUG -g")
  SET(CMAKE_C_FLAGS_RELEASE "-DNDEBUG -O2 -Wuninitialized")
ENDIF (WIN32)

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
  SET (header ${BASE}/${header})

  EXEC_PROGRAM(${CMAKE_COMMAND} ARGS -E copy_if_different ${header} ${INC_DIR})

  ADD_CUSTOM_COMMAND(TARGET yajl_s POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different ${header} ${INC_DIR})
ENDFOREACH (header ${PUB_HDRS})

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${INC_DIR}/..
)


