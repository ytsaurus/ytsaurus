set(NAME deps-arcadia-contrib-libs-libbz2)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2)

set(SRCS
    ${BASE}/blocksort.c
    ${BASE}/bzlib.c
    ${BASE}/compress.c
    ${BASE}/crctable.c
    ${BASE}/decompress.c
    ${BASE}/huffman.c
    ${BASE}/randtable.c
)

add_library(${NAME} STATIC ${SRCS})
