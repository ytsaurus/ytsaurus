
# This file was generated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.

add_library(server-lib-alert_manager)


target_compile_options(server-lib-alert_manager PRIVATE
  -Wdeprecated-this-capture
)

target_link_libraries(server-lib-alert_manager PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  cpp-yt-memory
  yt-yt-core
)

target_sources(server-lib-alert_manager PRIVATE
  ${PROJECT_SOURCE_DIR}/yt/yt/server/lib/alert_manager/alert_manager.cpp
  ${PROJECT_SOURCE_DIR}/yt/yt/server/lib/alert_manager/config.cpp
)






