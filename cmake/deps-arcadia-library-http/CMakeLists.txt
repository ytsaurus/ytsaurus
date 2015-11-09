set( BASE ${CMAKE_SOURCE_DIR}/library/http )
set( SRCS
  ${BASE}/server/conn.cpp
  ${BASE}/server/http.cpp
  ${BASE}/server/options.cpp
  ${BASE}/misc/parsed_request.cpp
  ${BASE}/io/stream.cpp
  ${BASE}/io/headers.cpp
  ${BASE}/io/chunk.cpp
)

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_library( ytext-arcadia-library-http STATIC ${SRCS} )

target_link_libraries( ytext-arcadia-library-http
  ytext-arcadia-util
)

