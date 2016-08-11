set(NAME deps-arcadia-library-lwtrace)
set(BASE ${CMAKE_SOURCE_DIR}/library/lwtrace)
set(SRCS
  ${BASE}/check.cpp
  ${BASE}/kill_action.cpp
  ${BASE}/sleep_action.cpp
  ${BASE}/start.cpp
  ${BASE}/stderr_writer.cpp
  ${BASE}/trace.cpp
)

protoc(${BASE}/lwtrace.proto SRCS)

include_directories(
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME} deps-arcadia-util)
