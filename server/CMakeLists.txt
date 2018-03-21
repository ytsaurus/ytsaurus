include(Sources.txt)

resolve_srcs(YP_SRCS YP_SRCS_RESOLVED)

add_library(yp-library-server ${YP_SRCS_RESOLVED})

target_link_libraries(yp-library-server
  yt-core-rpc-grpc
  yt-core-rpc-http
  yt-library-client
  yp-library-client
  deps-arcadia-library-http
)

### master

add_executable(ypserver-master
  ${CMAKE_CURRENT_LIST_DIR}/master_program/main.cpp
)

target_link_libraries(ypserver-master
  yp-library-server
  yp-build
  deps-arcadia-library-getopt
  deps-arcadia-library-lfalloc
)

install(
  TARGETS
    ypserver-master
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  COMPONENT yp
)

set_target_properties(ypserver-master PROPERTIES LINK_FLAGS "-static-libstdc++")

### udf

set(TARGETS "")

udf_bc(${CMAKE_CURRENT_LIST_DIR}/objects/udf/enums.cpp TARGETS
  SYMBOLS
    format_pod_current_state
    format_pod_target_state
    format_hfsm_state
  DEPENDS
    yt-core
    yt-core-codegen
)

add_custom_target(
    yp-server-udf
  ALL
  DEPENDS
    ${TARGETS}
  WORKING_DIRECTORY
    ${CMAKE_CURRENT_LIST_DIR}
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/yp/server/enums.bc
  ${CMAKE_CURRENT_LIST_DIR}/objects/udf/enums.bc
)

