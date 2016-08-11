set(NAME deps-arcadia-library-http)
set(BASE ${CMAKE_SOURCE_DIR}/library/http)
set(SRCS
  ${BASE}/server/conn.cpp
  ${BASE}/server/http.cpp
  ${BASE}/server/http_ex.cpp
  ${BASE}/server/options.cpp
  ${BASE}/misc/httpreqdata.cpp
  ${BASE}/misc/parsed_request.cpp
  ${BASE}/io/stream.cpp
  ${BASE}/io/headers.cpp
  ${BASE}/io/chunk.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME} deps-arcadia-util)
