include(Sources.txt)

if(YT_ADD_HEADERS_TO_SRCS)
  file(GLOB_RECURSE HEADERS ${CMAKE_CURRENT_LIST_DIR}/*.h)
  set(YT_SRCS ${YT_SRCS} ${HEADERS})
endif()

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_library(yt-library-server ${YT_SRCS_RESOLVED})

target_link_libraries(yt-library-server
  yt-library-ytlib
  yt-library-ytlib-auth
  yt-core-re2
  yt-core-rpc-grpc
  deps-arcadia-library-http
  deps-arcadia-contrib-libs-msgpack
  deps-arcadia-library-streams-lzop
  deps-arcadia-library-string_utils-base64
)

add_subdirectory(http_proxy)

################################################################################

add_executable(ytserver-master
  ${CMAKE_CURRENT_LIST_DIR}/cell_master_program/main.cpp
)

target_link_libraries(ytserver-master
  yt-core-phdr-cache
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-master PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-node
  ${CMAKE_CURRENT_LIST_DIR}/cell_node_program/main.cpp
)

target_link_libraries(ytserver-node
  yt-core-phdr-cache
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-node PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-scheduler
  ${CMAKE_CURRENT_LIST_DIR}/programs/scheduler/main.cpp
)

target_link_libraries(ytserver-scheduler
  yt-core-phdr-cache
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-scheduler PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-controller-agent
  ${CMAKE_CURRENT_LIST_DIR}/programs/controller_agent/main.cpp
)

target_link_libraries(ytserver-controller-agent
  yt-core-phdr-cache
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-controller-agent PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-proxy
  ${CMAKE_CURRENT_LIST_DIR}/cell_proxy_program/main.cpp
)

target_link_libraries(ytserver-proxy
  yt-core-phdr-cache
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-proxy PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-http-proxy
  ${CMAKE_CURRENT_LIST_DIR}/http_proxy_program/main.cpp
)

target_link_libraries(ytserver-http-proxy
  yt-library-server
  yt-library-server-http-proxy
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-http-proxy PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-job-proxy
  ${CMAKE_CURRENT_LIST_DIR}/job_proxy_program/main.cpp
)

target_link_libraries(ytserver-job-proxy
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-job-proxy PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-exec
  ${CMAKE_CURRENT_LIST_DIR}/exec_program/main.cpp
)

target_link_libraries(ytserver-exec
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-exec PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-tools
  ${CMAKE_CURRENT_LIST_DIR}/tools_program/main.cpp
)

target_link_libraries(ytserver-tools
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-tools PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-core-forwarder
  ${CMAKE_CURRENT_LIST_DIR}/core_forwarder/main.cpp
)

target_link_libraries(ytserver-core-forwarder
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-core-forwarder PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

add_executable(ytserver-skynet-manager
  ${CMAKE_CURRENT_LIST_DIR}/skynet_manager_program/main.cpp
)

target_link_libraries(ytserver-skynet-manager
  yt-core-phdr-cache
  yt-library-server
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

set_target_properties(ytserver-skynet-manager PROPERTIES LINK_FLAGS "-static-libstdc++")

################################################################################

if(YT_BUILD_ENABLE_GDB_INDEX AND NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_gdb_index(TARGETS
      ytserver-master ytserver-scheduler ytserver-node ytserver-controller-agent
      ytserver-job-proxy ytserver-exec ytserver-core-forwarder ytserver-tools
  )
endif()

install(
  TARGETS ytserver-master
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  COMPONENT cell_master
)


install(
  TARGETS ytserver-scheduler
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  COMPONENT scheduler
)

install(
  TARGETS ytserver-controller-agent
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  COMPONENT controller_agent
)

install(
  TARGETS
    ytserver-node ytserver-job-proxy ytserver-exec
    ytserver-core-forwarder ytserver-tools
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  COMPONENT cell_node
)

install(
  TARGETS
    ytserver-proxy ytserver-http-proxy
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  COMPONENT cell_proxy
)

install(
  TARGETS
    ytserver-master ytserver-scheduler ytserver-node ytserver-proxy ytserver-controller-agent
    ytserver-job-proxy ytserver-exec ytserver-core-forwarder ytserver-tools
    ytserver-skynet-manager ytserver-http-proxy
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  COMPONENT all_binaries
)
