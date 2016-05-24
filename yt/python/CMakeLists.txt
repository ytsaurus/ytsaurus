IF(YMAKE)
  SET(CMAKE_CURRENT_LIST_DIR .)
  SET(YT_ABI 17_4)
ENDIF()

SET(PROTO_NAMESPACE yt/${YT_ABI})

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
  yt/${YT_ABI}/core
  yt/${YT_ABI}/ytlib
)

PY_REGISTER(yson_lib)

END()

################################################################################
ELSE()
################################################################################

include_directories(${PYTHON_INCLUDE_DIR})

add_library(yt-driver-python MODULE
  ${YT_SRCS}
  ${CMAKE_CURRENT_LIST_DIR}/driver.cpp
)

add_library(yt-yson-python MODULE
  ${YT_SRCS}
  ${CMAKE_CURRENT_LIST_DIR}/yson.cpp
)

target_link_libraries(yt-driver-python
  ytlib
  ytext-pycxx
  ${PYTHON_LIBRARY}
)

target_link_libraries(yt-yson-python
  ytlib
  ytext-pycxx
  ${PYTHON_LIBRARY}
)

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

set_target_properties(yt-driver-python PROPERTIES LINK_FLAGS ${PYTHON_LINK_FLAGS})

set_target_properties(yt-yson-python PROPERTIES LINK_FLAGS ${PYTHON_LINK_FLAGS})

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-driver-python${CMAKE_SHARED_MODULE_SUFFIX}
  ${CMAKE_SOURCE_DIR}/python/yt_driver_bindings/driver_lib${CMAKE_SHARED_MODULE_SUFFIX}
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_BINARY_DIR}/lib/libyt-yson-python${CMAKE_SHARED_MODULE_SUFFIX}
  ${CMAKE_SOURCE_DIR}/python/yt_yson_bindings/yson_lib${CMAKE_SHARED_MODULE_SUFFIX}
)

################################################################################
ENDIF()
################################################################################

