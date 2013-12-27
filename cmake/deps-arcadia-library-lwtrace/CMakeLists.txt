set( BASE ${CMAKE_SOURCE_DIR}/library/lwtrace )
set( SRCS
  ${BASE}/check.cpp
  ${BASE}/kill_action.cpp
  ${BASE}/start.cpp
  ${BASE}/stderr_writer.cpp
  ${BASE}/trace.cpp
)

protoc( ${BASE}/lwtrace.proto SRCS )

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_library( ytext-arcadia-library-lwtrace STATIC ${SRCS} )

target_link_libraries( ytext-arcadia-library-lwtrace ytext-arcadia-util )

