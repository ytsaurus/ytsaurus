set(NAME deps-arcadia-contrib-libs-gtest)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/gtest)
set(SRCS
  ${BASE}/src/gtest-all.cc
)

include_directories(${BASE})

add_library(${NAME} STATIC ${SRCS})
