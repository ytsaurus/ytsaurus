set( LTYPE STATIC )

set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/pycxx )

add_library( ytext-pycxx ${LTYPE}
  ${BASE}/cxx_extensions.cxx
  ${BASE}/cxxextensions.c
  ${BASE}/cxxsupport.cxx
  ${BASE}/IndirectPythonInterface.cxx
)

include_directories(
  ${BASE}
  ${CMAKE_SOURCE_DIR}
  /usr/include/python2.7/
)

set_target_properties( ytext-pycxx PROPERTIES
  VERSION   6.2.4
  SOVERSION 0.1
)

install(
  TARGETS
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)

