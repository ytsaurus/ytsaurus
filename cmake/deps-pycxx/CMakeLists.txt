if (NOT PYTHONLIBS_FOUND)
  message(FATAL_ERROR "Python libraries are required to build PyCxx")
endif()

if (NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR "Python interpreter is required to build PyCxx")
endif()

set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/pycxx )

add_library( ytext-pycxx STATIC
  ${BASE}/cxx_extensions.cxx
  ${BASE}/cxxextensions.c
  ${BASE}/cxxsupport.cxx
  ${BASE}/IndirectPythonInterface.cxx
)

include_directories(
  ${BASE}
  ${CMAKE_SOURCE_DIR}
  ${PYTHON_INCLUDE_DIRS}
)

set_target_properties( ytext-pycxx PROPERTIES
  VERSION   6.2.4
  SOVERSION 6.2
)
