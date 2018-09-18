# Build library for each kind of Python.
foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
  get_python_kind_specific_constants(${PYTHON_KIND} PYTHON_KIND_DASHED TARGET_NAME_PREFIX)
endforeach()
