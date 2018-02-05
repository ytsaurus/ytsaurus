SET(YT_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/buffered_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/shutdown.cpp
  ${CMAKE_CURRENT_LIST_DIR}/stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lazy_dict.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object_builder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson_lazy_map.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lazy_list_fragment_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lazy_yson_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lazy_dict_producer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lazy_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/list_fragment_parser.cpp
)

if (_is_gcc)
  set_source_files_properties(${CMAKE_CURRENT_LIST_DIR}/serialize.cpp PROPERTIES COMPILE_FLAGS -Wno-write-strings)
  set_source_files_properties(${CMAKE_CURRENT_LIST_DIR}/yson_lazy_map.cpp PROPERTIES COMPILE_FLAGS -Wno-write-strings)
  if (_is_clang)
    set_source_files_properties(${CMAKE_CURRENT_LIST_DIR}/serialize.cpp PROPERTIES COMPILE_FLAGS "-Wno-parentheses-equality -Wno-writable-strings")
  endif()
endif()

execute_process(COMMAND lsb_release --codename --short
                OUTPUT_VARIABLE LSB_RELEASE)

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

set(PYCXX_BASE ${CMAKE_SOURCE_DIR}/contrib/libs/pycxx)
set(PYCXX_SRCS
  ${PYCXX_BASE}/cxx_exceptions.cxx
  ${PYCXX_BASE}/cxx_extensions.cxx
  ${PYCXX_BASE}/cxxextensions.c
  ${PYCXX_BASE}/cxxsupport.cxx
  ${PYCXX_BASE}/IndirectPythonInterface.cxx
)

# Build library for each kind of Python.
foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
  string(TOLOWER ${PYTHON_KIND} PYTHON_KIND_LOWER)
  string(REPLACE "_" "-" PYTHON_KIND_DASHED ${PYTHON_KIND_LOWER})
  set(TARGET_NAME_PREFIX "yt-python-${PYTHON_KIND_DASHED}-")

  set(IS_NATIVE_SYSTEM_PYTHON False)
  if(${PYTHON_KIND} MATCHES "^2_7$")
    if(${LSB_RELEASE} MATCHES "precise")
      set(IS_NATIVE_SYSTEM_PYTHON True)
    endif()
    if(${LSB_RELEASE} MATCHES "trusty")
      set(IS_NATIVE_SYSTEM_PYTHON True)
    endif()
    if(${LSB_RELEASE} MATCHES "xenial")
      set(IS_NATIVE_SYSTEM_PYTHON True)
    endif()
  endif()
  if(${PYTHON_KIND} MATCHES "^2_6$")
    if(${LSB_RELEASE} MATCHES "lucid")
      set(IS_NATIVE_SYSTEM_PYTHON True)
    endif()
  endif()

  add_library(${TARGET_NAME_PREFIX}pycxx STATIC ${PYCXX_SRCS})
  target_include_directories(${TARGET_NAME_PREFIX}pycxx PUBLIC ${PYCXX_BASE} ${PYTHON_${PYTHON_KIND}_INCLUDE_DIR})
  target_compile_definitions(${TARGET_NAME_PREFIX}pycxx
    PUBLIC
      -DPYCXX_PYTHON_2TO3
      -DPYCXX_6_2_COMPATIBILITY
      -DPy_LIMITED_API=0x03040000)

  add_library(${TARGET_NAME_PREFIX}common STATIC ${YT_SRCS})
  target_link_libraries(${TARGET_NAME_PREFIX}common yt-core ${TARGET_NAME_PREFIX}pycxx)

  add_library(${TARGET_NAME_PREFIX}yson
    MODULE
    ${CMAKE_CURRENT_LIST_DIR}/yson.cpp
  )

  target_link_libraries(${TARGET_NAME_PREFIX}yson ${TARGET_NAME_PREFIX}common)

  add_library(${TARGET_NAME_PREFIX}driver
    MODULE
    ${CMAKE_CURRENT_LIST_DIR}/driver.cpp
    ${CMAKE_CURRENT_LIST_DIR}/descriptor.cpp
    ${CMAKE_CURRENT_LIST_DIR}/response.cpp
  )

  target_link_libraries(${TARGET_NAME_PREFIX}driver yt-library-client ${TARGET_NAME_PREFIX}common)

  foreach(COMPONENT "yson" "driver")
    set(TARGET_NAME "${TARGET_NAME_PREFIX}${COMPONENT}")

    target_compile_definitions(${TARGET_NAME} PUBLIC -DPy_LIMITED_API=0x03040000)

    set_target_properties(${TARGET_NAME}
      PROPERTIES
      LIBRARY_OUTPUT_NAME "${COMPONENT}_lib"
      LIBRARY_OUTPUT_DIRECTORY "${LIBRARY_OUTPUT_PATH}/pyshared-${PYTHON_KIND_DASHED}"
      PREFIX ""
      SUFFIX "${CMAKE_SHARED_MODULE_SUFFIX}"
      LINK_FLAGS "${PYTHON_LINK_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS}"
    )

    install(
      TARGETS ${TARGET_NAME}
      DESTINATION lib/pyshared-${PYTHON_KIND_DASHED}/yt_${COMPONENT}_bindings
      COMPONENT ${TARGET_NAME}
    )

    if(${IS_NATIVE_SYSTEM_PYTHON})
      exec_program(
        ${CMAKE_COMMAND} ARGS -E create_symlink
        ${LIBRARY_OUTPUT_PATH}/pyshared-${PYTHON_KIND_DASHED}/${COMPONENT}_lib${CMAKE_SHARED_MODULE_SUFFIX}
        ${CMAKE_SOURCE_DIR}/python/yt_${COMPONENT}_bindings/${COMPONENT}_lib${CMAKE_SHARED_MODULE_SUFFIX}
      )
    endif()
  endforeach()

  install(
    FILES
      ${CMAKE_SOURCE_DIR}/python/yt_driver_bindings/__init__.py
      ${CMAKE_SOURCE_DIR}/python/yt_driver_bindings/driver.py
    DESTINATION
      lib/pyshared-${PYTHON_KIND_DASHED}/yt_driver_bindings
      COMPONENT ${TARGET_NAME_PREFIX}driver
  )

endforeach()
