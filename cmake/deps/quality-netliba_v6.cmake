if (UNIX)
  SET(LTYPE SHARED)
endif()

if (WIN32)
  SET(LTYPE STATIC)
endif()

set( BASE ${CMAKE_SOURCE_DIR}/quality/netliba_v6 )

add_library( ytext-quality-netliba_v6 ${LTYPE}
  ${BASE}/stdafx.cpp
  ${BASE}/udp_address.cpp
  ${BASE}/udp_client_server.cpp
  ${BASE}/udp_http.cpp
  ${BASE}/net_acks.cpp
  ${BASE}/udp_test.cpp
  ${BASE}/block_chain.cpp
  ${BASE}/net_test.cpp
  ${BASE}/udp_debug.cpp
  ${BASE}/udp_socket.cpp
)

target_link_libraries( ytext-quality-netliba_v6
  ytext-arcadia-util
  ytext-quality-misc
)

include_directories(
  ${CMAKE_SOURCE_DIR}
)

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( ytext-quality-netliba_v6 stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( ytext-quality-netliba_v6 PROPERTIES LINK_FLAGS "-nodefaultlibs -L${CMAKE_BINARY_DIR}/lib" )
  endif()
endif()

install(
  TARGETS ytext-quality-netliba_v6
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
