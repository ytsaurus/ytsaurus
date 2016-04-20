IF(YMAKE)
  SET(CMAKE_CURRENT_LIST_DIR .)
ENDIF()

SET(YT_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/buffered_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/descriptor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/response.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/shutdown.cpp
  ${CMAKE_CURRENT_LIST_DIR}/stream.cpp
)

################################################################################
IF(YMAKE)
################################################################################

LIBRARY()

OWNER(sandello)

NO_COMPILER_WARNINGS()
NO_WERROR()

PYTHON_ADDINCL()

SRCS(
  ${YT_SRCS}
  ${CMAKE_CURRENT_LIST_DIR}/yson.cpp
)

PEERDIR(
  contrib/libs/pycxx
  yt/core
  yt/ytlib
)

PY_REGISTER(yson_lib)

END()

################################################################################
ELSE()
################################################################################

set(PYTHON_LINK_FLAGS "-static-libstdc++")

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set(
    PYTHON_LINK_FLAGS
    "${PYTHON_LINK_FLAGS} -Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/linker.map.darwin")
else()
  set(
    PYTHON_LINK_FLAGS
    "${PYTHON_LINK_FLAGS} -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/linker.map")
endif()

# Determine kinds of Python to build with.
set(PYTHON_KINDS)
foreach(PYTHON_KIND "PYTHON" "PYTHON_SKYNET")
  if(${PYTHON_KIND}_INCLUDE_DIR)
    list(APPEND PYTHON_KINDS ${PYTHON_KIND})
  endif()
endforeach()

# Build library for each kind of Python.
foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
  string(TOLOWER ${PYTHON_KIND} PYTHON_KIND_LOWER)
  string(REPLACE "_" "-" PYTHON_KIND_SUFFIX ${PYTHON_KIND_LOWER})

  foreach(COMPONENT "yson" "driver")
    set(TARGET_NAME "yt-${COMPONENT}-${PYTHON_KIND_SUFFIX}")

    add_library(${TARGET_NAME} MODULE
      ${YT_SRCS}
      ${CMAKE_CURRENT_LIST_DIR}/${COMPONENT}.cpp
    )

    target_include_directories(${TARGET_NAME} PUBLIC
      ${${PYTHON_KIND}_INCLUDE_DIR}
    )

    target_link_libraries(${TARGET_NAME}
      ytlib
      ytext-pycxx
      ${${PYTHON_KIND}_LIBRARY}
    )

    set_target_properties(${TARGET_NAME}
      PROPERTIES
      LINK_FLAGS ${PYTHON_LINK_FLAGS}
    )

    if(${PYTHON_VERSION} MATCHES "^PYTHON$")
      exec_program(
        ${CMAKE_COMMAND} ARGS -E create_symlink
        ${CMAKE_BINARY_DIR}/lib/lib${TARGET_NAME}${CMAKE_SHARED_MODULE_SUFFIX}
        ${CMAKE_SOURCE_DIR}/python/yt_${COMPONENT}_bindings/${COMPONENT}_lib${CMAKE_SHARED_MODULE_SUFFIX}
      )
    endif()
  endforeach()
endforeach()

################################################################################
ENDIF()
################################################################################

