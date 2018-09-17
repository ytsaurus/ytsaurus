set(NAME deps-arcadia-library-uri)
set(BASE ${CMAKE_SOURCE_DIR}/library/uri)

set(SRCS
  ${BASE}/assign.cpp
  ${BASE}/common.cpp
  ${BASE}/encode.cpp
  ${BASE}/encodefsm.cpp
  ${BASE}/location.cpp
  ${BASE}/other.cpp
  ${BASE}/parse.cpp
  ${BASE}/parsefsm.cpp
  ${BASE}/qargs.cpp
  ${BASE}/uri.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-util
  deps-arcadia-library-charset
  deps-arcadia-contrib-libs-libiconv
  deps-arcadia-contrib-libs-libidn
)

