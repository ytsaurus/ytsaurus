set(NAME deps-arcadia-contrib-libs-gmock)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/gmock)
set(SRCS
  ${BASE}/src/gmock-all.cc
)

include_directories(
  ${CMAKE_SOURCE_DIR}/contrib/libs/gtest
  ${CMAKE_SOURCE_DIR}/contrib/libs/gmock
)

add_library(${NAME} STATIC ${SRCS})
