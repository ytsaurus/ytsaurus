set(NAME deps-arcadia-library-http)
set(BASE ${CMAKE_SOURCE_DIR}/library/http)

set(SRCS
  ${BASE}/io/chunk.cpp
  ${BASE}/io/headers.cpp
  ${BASE}/io/stream.cpp
  ${BASE}/misc/httpreqdata.cpp
  ${BASE}/misc/parsed_request.cpp
  ${BASE}/server/conn.cpp
  ${BASE}/server/http.cpp
  ${BASE}/server/http_ex.cpp
  ${BASE}/server/options.cpp
  ${BASE}/simple/http_client.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-library-openssl
  deps-arcadia-library-uri
  deps-arcadia-util
)
