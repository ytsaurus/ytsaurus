cmake_minimum_required(VERSION 3.5)

project(ytsaurus_cpp_api)

set(CMAKE_CXX_STANDARD 20)
set(Protobuf_USE_STATIC_LIBS On CACHE INTERNAL "")
set(CMAKE_SHARED_LINKER_FLAGS "-static-libgcc -static-libstdc++ -static" On CACHE INTERNAL "")

include_directories(${PROJECT_SOURCE_DIR}/ytsaurus)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/ytsaurus/ytsaurus/cmake/)

add_subdirectory(ytsaurus)

find_package(Python3 3.7 EXACT COMPONENTS Interpreter)

add_definitions(-DUSE_SYSTEM_STL)

# set(EXTERNAL_LIB_HEADERS ytsaurus)

include(FindProtobuf)
find_package(Protobuf REQUIRED)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${Protobuf_INCLUDE_DIRS})
