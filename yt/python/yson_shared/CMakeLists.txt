include(Sources.txt)

# Build library for each kind of Python.
foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
  get_python_kind_specific_constants(${PYTHON_KIND} PYTHON_KIND_DASHED TARGET_NAME_PREFIX)

  add_library(${TARGET_NAME_PREFIX}yson-shared MODULE ${YT_SRCS})
  target_link_libraries(${TARGET_NAME_PREFIX}yson-shared
    yt-core
    ${TARGET_NAME_PREFIX}pycxx
    ${TARGET_NAME_PREFIX}common
    ${TARGET_NAME_PREFIX}yson)

  build_and_install_python_library(${PYTHON_KIND} "${TARGET_NAME_PREFIX}yson-shared" "yson")
endforeach()
