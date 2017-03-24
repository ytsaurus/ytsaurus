set(NAME deps-yt-contrib-portoapi)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/portoapi)
include_directories(${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR}/yt/contrib/portoapi/)
include_directories(${CMAKE_BINARY_DIR})
set(SRCS
    ${BASE}/libporto.cpp
    ${BASE}/libporto.hpp
)

protoc(${BASE}/rpc.proto SRCS)
protoc(${BASE}/kv.proto SRCS)
protoc(${BASE}/config.proto SRCS)

add_library(${NAME} STATIC ${SRCS})
