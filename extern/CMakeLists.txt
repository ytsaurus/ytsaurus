cmake_minimum_required (VERSION 2.6)

PROJECT(stlport)

if (UNIX)
  ADD_CUSTOM_TARGET(
    release-shared ALL make -f gcc.mak BASE_INSTALL_DIR=${CMAKE_SOURCE_DIR} install-release-shared
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/STLport/build/lib
    COMMENT STLport
  )
  ADD_CUSTOM_TARGET(
    depend ALL make -f gcc.mak depend
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/STLport/build/lib
    COMMENT STLport
  )
  # ADD_DEPENDENCIES( depend stlp-depend )
endif (UNIX)

if (WIN32)
  ADD_CUSTOM_TARGET(
    static ALL nmake /F msvc.mak INSTALL_LIB_DIR=..\\..\\lib\\ INSTALL_BIN_DIR=..\\..\\bin\\ INSTALL_LIB_DIR_DBG=..\\..\\lib\\ INSTALL_BIN_DIR_DBG=..\\..\\bin\\ install-release-static install-dbg-static
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/STLport/build/lib
    COMMENT STLport
  )
endif (WIN32)
