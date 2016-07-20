set(NAME deps-arcadia-library-string_utils-base64)
set(BASE ${CMAKE_SOURCE_DIR}/library/string_utils/base64)
set(SRCS
  ${BASE}/base64.cpp
)

include_directories(${CMAKE_SOURCE_DIR} ${BASE})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  ytext-arcadia-util
)
