include(Sources.txt)

# Build library for each kind of Python.
foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
  get_python_kind_specific_constants(${PYTHON_KIND} PYTHON_KIND_DASHED TARGET_NAME_PREFIX)

  add_library(${TARGET_NAME_PREFIX}driver MODULE ${YT_SRCS})
  target_link_libraries(${TARGET_NAME_PREFIX}driver yt-library-ytlib ${TARGET_NAME_PREFIX}common ${TARGET_NAME_PREFIX}yson)

  build_and_install_python_library(${PYTHON_KIND} "${TARGET_NAME_PREFIX}driver" "driver")

  install(
    FILES
      ${CMAKE_SOURCE_DIR}/python/yt_driver_bindings/__init__.py
      ${CMAKE_SOURCE_DIR}/python/yt_driver_bindings/driver.py
    DESTINATION
      lib/pyshared-${PYTHON_KIND_DASHED}/yt_driver_bindings
      COMPONENT ${TARGET_NAME_PREFIX}driver
  )

endforeach()
