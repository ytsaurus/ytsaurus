set_directory_properties( PROPERTIES INCLUDE_DIRECTORIES "" )

set( BASE ${CMAKE_SOURCE_DIR}/contrib/libev )
string( TOLOWER ${CMAKE_SYSTEM_NAME} STYPE )

add_library( ytext-ev STATIC ${BASE}/ev.c )

add_definitions(
  -DEV_CONFIG_H="config_${STYPE}.h"
  -D_LARGEFILE_SOURCE
  -D_FILE_OFFSET_BITS=64
)

if ( FALSE )
elseif( STYPE STREQUAL "darwin" )
  add_definitions( -D_DARWIN_USE_64_BIT_INODE=1 )
elseif( STYPE STREQUAL "linux" )
  add_definitions( -D_GNU_SOURCE )
  set_target_properties( ytext-ev PROPERTIES
    COMPILE_FLAGS "-Wno-unused-value -Wno-comment")
  target_link_libraries( ytext-ev -lm -lrt )
elseif( STYPE STREQUAL "windows" )
  set_target_properties( ytext-ev PROPERTIES
    COMPILE_DEFINITIONS "_WIN32_WINNT=0x0501;HAVE_ERRNO_H")
endif()

include_directories( ${BASE} )

