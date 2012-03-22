set( SRCS
  main.cpp
  scheduler_bootstrap.cpp
)

add_msvc_precompiled_header( "stdafx.h" "stdafx.cpp" SRCS )
add_executable( ytserver ${SRCS} )

target_link_libraries( ytserver
  ytlib
)

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( ytserver stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( ytserver PROPERTIES LINK_FLAGS "-nodefaultlibs -L${CMAKE_BINARY_DIR}/lib" )
  endif()
endif()

install(
  TARGETS ytserver
  RUNTIME DESTINATION bin
)
