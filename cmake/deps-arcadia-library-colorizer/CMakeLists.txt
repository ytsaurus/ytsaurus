set(NAME deps-arcadia-library-colorizer)
set(BASE ${CMAKE_SOURCE_DIR}/library/colorizer)
set(SRCS
  ${BASE}/colors.cpp
  ${BASE}/output.cpp
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${BASE}
)

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME} deps-arcadia-util)
