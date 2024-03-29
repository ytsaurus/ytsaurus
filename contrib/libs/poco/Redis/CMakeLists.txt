
# This file was generated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.

add_library(libs-poco-Redis)


target_compile_options(libs-poco-Redis PRIVATE
  -DPOCO_ENABLE_CPP11
  -DPOCO_ENABLE_CPP14
  -DPOCO_NO_AUTOMATIC_LIBS
  -DPOCO_UNBUNDLED
  -DPOCO_OS_FAMILY_UNIX
  -DPOCO_HAVE_FD_EPOLL
  $<IF:$<CXX_COMPILER_ID:MSVC>,,-Wno-everything>
)

target_include_directories(libs-poco-Redis PUBLIC
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/include
)
target_include_directories(libs-poco-Redis PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Foundation/include
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Net/include
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src
)

target_link_libraries(libs-poco-Redis PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  libs-poco-Foundation
  libs-poco-Net
)

target_sources(libs-poco-Redis PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/Array.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/AsyncReader.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/Client.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/Command.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/Error.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/Exception.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/RedisEventArgs.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/RedisStream.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/poco/Redis/src/Type.cpp
)






