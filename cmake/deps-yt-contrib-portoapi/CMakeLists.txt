set(NAME deps-yt-contrib-portoapi)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/portoapi)
set(SRCS
  ${BASE}/libporto.cpp
  ${BASE}/libporto.hpp
)

protoc(${BASE}/rpc.proto SRCS)
protoc(${BASE}/kv.proto SRCS)
protoc(${BASE}/config.proto SRCS)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/google/protobuf
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/yt/contrib/portoapi
)

add_library(${NAME} STATIC ${SRCS})
