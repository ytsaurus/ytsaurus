include(Sources.txt)

if (_is_gcc)
  set_source_files_properties(${CMAKE_CURRENT_LIST_DIR}/serialize.cpp PROPERTIES COMPILE_FLAGS -Wno-write-strings)
  set_source_files_properties(${CMAKE_CURRENT_LIST_DIR}/yson_lazy_map.cpp PROPERTIES COMPILE_FLAGS -Wno-write-strings)
  if (_is_clang)
    set_source_files_properties(${CMAKE_CURRENT_LIST_DIR}/serialize.cpp PROPERTIES COMPILE_FLAGS "-Wno-parentheses-equality -Wno-writable-strings")
  endif()
endif()

# Build library for each kind of Python.
foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
  get_python_kind_specific_constants(${PYTHON_KIND} PYTHON_KIND_DASHED TARGET_NAME_PREFIX)

  add_library(${TARGET_NAME_PREFIX}yson STATIC ${YT_SRCS})
  target_link_libraries(${TARGET_NAME_PREFIX}yson ${TARGET_NAME_PREFIX}common)

  add_library(${TARGET_NAME_PREFIX}yson-shared MODULE yson.cpp)
  target_link_libraries(${TARGET_NAME_PREFIX}yson-shared
    yt-core
    ${TARGET_NAME_PREFIX}pycxx
    ${TARGET_NAME_PREFIX}common
    ${TARGET_NAME_PREFIX}yson)

  build_and_install_python_library(${PYTHON_KIND} "${TARGET_NAME_PREFIX}yson-shared" "yson")
endforeach()
