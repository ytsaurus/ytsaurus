set(NAME deps-yt-contrib-fastlz)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/fastlz)
set(SRCS
  ${BASE}/fastlz.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

