set(NAME deps-arcadia-contrib-libs-yajl)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/yajl)
set(SRCS_C
  ${BASE}/yajl_alloc.c
  ${BASE}/yajl_buf.c
  ${BASE}/yajl.c
  ${BASE}/yajl_encode.c
  ${BASE}/yajl_gen.c
  ${BASE}/yajl_lex.c
  ${BASE}/yajl_parser.c
  ${BASE}/yajl_tree.c
  ${BASE}/yajl_version.c
)
set(SRCS_CXX
  ${BASE}/yajl_parser.cpp
)

set_source_files_properties(${SRCS_C} PROPERTIES LANGUAGE C)
set_source_files_properties(${SRCS_CXX} PROPERTIES LANGUAGE CXX)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS_C} ${SRCS_CXX})

if(CMAKE_COMPILER_IS_GNUCXX)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -w)
endif()

