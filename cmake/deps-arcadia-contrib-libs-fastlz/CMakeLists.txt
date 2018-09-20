set(NAME deps-arcadia-contrib-libs-fastlz)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/fastlz)
set(SRCS
  ${BASE}/fastlz.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

