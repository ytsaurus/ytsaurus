set( BASE ${CMAKE_SOURCE_DIR}/yt/python )
set( SRCS
  ${BASE}/buffered_stream.cpp
  ${BASE}/helpers.cpp
  ${BASE}/descriptor.cpp
  ${BASE}/response.cpp
  ${BASE}/serialize.cpp
  ${BASE}/shutdown.cpp
  ${BASE}/stream.cpp
)

set( HDRS
  ${BASE}/buffered_stream.h
  ${BASE}/helpers.h
  ${BASE}/descriptor.h
  ${BASE}/public.h
  ${BASE}/response.h
  ${BASE}/serialize.h
  ${BASE}/shutdown.h
  ${BASE}/stream.h
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
      ${BASE}/${COMPONENT}.cpp
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
