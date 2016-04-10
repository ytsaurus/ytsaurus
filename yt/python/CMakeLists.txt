set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/buffered_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/descriptor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/response.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/shutdown.cpp
  ${CMAKE_CURRENT_LIST_DIR}/stream.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/buffered_stream.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/descriptor.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/response.h
  ${CMAKE_CURRENT_LIST_DIR}/serialize.h
  ${CMAKE_CURRENT_LIST_DIR}/shutdown.h
  ${CMAKE_CURRENT_LIST_DIR}/stream.h
)

set(PYTHON_LINK_FLAGS "-static-libstdc++")

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set( PYTHON_LINK_FLAGS "${PYTHON_LINK_FLAGS} -Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/linker.map.darwin" )
else()
  set( PYTHON_LINK_FLAGS "${PYTHON_LINK_FLAGS} -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/linker.map" )
endif()

# Determine versions of python to build with.
set(PYTHON_VERSIONS)
foreach(PYTHON_VERSION "PYTHON" "PYTHON_SKYNET")
  # TODO(ignat): implement with continue() once cmake will be updated to newer version.
  if(${PYTHON_VERSION}_INCLUDE_DIR)
    list(APPEND PYTHON_VERSIONS ${PYTHON_VERSION})
  endif()
endforeach()

# Build library for each python.
foreach(PYTHON_VERSION IN LISTS PYTHON_VERSIONS)
  string(TOLOWER ${PYTHON_VERSION} PYTHON_VERSION_LOWER)
  string(REPLACE "_" "-" PYTHON_LIB_VERSION ${PYTHON_VERSION_LOWER})

  foreach(COMPONENT "yson" "driver")
    set(LIB_NAME "yt-${COMPONENT}-${PYTHON_LIB_VERSION}")

    add_library(${LIB_NAME} MODULE
      ${CMAKE_CURRENT_LIST_DIR}/${COMPONENT}.cpp
      ${SRCS}
    )

    target_include_directories(${LIB_NAME} PUBLIC
      ${${PYTHON_VERSION}_INCLUDE_DIR}
    )

    target_link_libraries(${LIB_NAME}
      ytlib
      ytext-pycxx
      ${${PYTHON_VERSION}_LIBRARY}
    )

    set_target_properties(${LIB_NAME}
      PROPERTIES
      LINK_FLAGS ${PYTHON_LINK_FLAGS}
    )

    if(${PYTHON_VERSION} MATCHES "^PYTHON$")
      exec_program(
        ${CMAKE_COMMAND} ARGS -E create_symlink
        ${CMAKE_BINARY_DIR}/lib/lib${LIB_NAME}${CMAKE_SHARED_MODULE_SUFFIX}
        ${CMAKE_SOURCE_DIR}/python/yt_${COMPONENT}_bindings/${COMPONENT}_lib${CMAKE_SHARED_MODULE_SUFFIX}
      )
    endif()
  endforeach(COMPONENT)
endforeach(PYTHON_VERSION)
