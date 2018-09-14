set(NAME deps-arcadia-contrib-libs-libiconv)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/libiconv)
set(SRCS
  ${BASE}/genaliases.c
  ${BASE}/genaliases2.c
  #genflags.c
  #gentranslit.c
  ${BASE}/iconv.c
  #relocatable.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

if (_is_gcc)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -Wno-error)
endif()
