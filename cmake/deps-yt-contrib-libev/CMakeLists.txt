set(NAME yt-contrib-libev)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/libev)
set(SRCS ${BASE}/ev.c)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${BASE}/ev.c)

string(TOLOWER ${CMAKE_SYSTEM_NAME} STYPE)
add_definitions(
  -DEV_CONFIG_H="config_${STYPE}.h"
  -D_LARGEFILE_SOURCE
  -D_FILE_OFFSET_BITS=64
)

if(FALSE)
elseif(STYPE STREQUAL "darwin")
  add_definitions(-D_DARWIN_USE_64_BIT_INODE=1)
elseif(STYPE STREQUAL "linux")
  add_definitions(-D_GNU_SOURCE)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS "-w")
  target_link_libraries(${NAME} -lm -lrt)
elseif(STYPE STREQUAL "windows")
  set_target_properties(${NAME} PROPERTIES COMPILE_DEFINITIONS "_WIN32_WINNT=0x0501;HAVE_ERRNO_H")
endif()

if (_is_clang)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -Wno-error)
endif()
