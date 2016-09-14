set(NAME deps-arcadia-contrib-libs-lzmasdk)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/lzmasdk)

set(SRCS
    ${BASE}/7zStream.c
    ${BASE}/7zStream.c
    ${BASE}/Alloc.c
    ${BASE}/LzmaDec.c
    ${BASE}/LzmaEnc.c
    ${BASE}/LzFind.c
    ${BASE}/LzmaLib.c
    ${BASE}/LzmaUtil.c
)

add_library(${NAME} STATIC ${SRCS})
