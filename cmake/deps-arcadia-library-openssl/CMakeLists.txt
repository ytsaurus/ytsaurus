set(NAME deps-arcadia-library-openssl)
set(BASE ${CMAKE_SOURCE_DIR}/library/openssl)
set(SRCS
  ${BASE}/io/stream.cpp
  ${BASE}/init/init.cpp
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/openssl/include
)

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-contrib-libs-openssl
  deps-arcadia-util
)
