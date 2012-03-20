set( SRCS
  main.cpp
)

add_executable( ytdriver ${SRCS} )

target_link_libraries( ytdriver
  ytlib
)

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( ytdriver stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( ytdriver PROPERTIES LINK_FLAGS "-nodefaultlibs -L${CMAKE_BINARY_DIR}/lib" )
  endif()
endif()

install(
  TARGETS ytdriver
  RUNTIME DESTINATION bin
)
