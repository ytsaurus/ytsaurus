include(Sources.txt)

if(YT_ADD_HEADERS_TO_SRCS)
    file(GLOB_RECURSE HEADERS ${CMAKE_CURRENT_LIST_DIR}/*.h)
    set(YT_SRCS ${YT_SRCS} ${HEADERS})
endif()

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-core-phdr-cache ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core-phdr-cache
  yt-core
)
