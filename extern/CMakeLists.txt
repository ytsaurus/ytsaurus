cmake_minimum_required (VERSION 2.6)

PROJECT(stlport)

if (UNIX)
  ADD_CUSTOM_TARGET(
    release-shared ALL make -f gcc.mak INSTALL_LIB_DIR=${CMAKE_SOURCE_DIR}/lib install-release-shared
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/STLport/build/lib
    COMMENT STLport
  )
  ADD_CUSTOM_TARGET(
    depend ALL make -f gcc.mak INSTALL_LIB_DIR=${CMAKE_SOURCE_DIR}/lib depend
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/STLport/build/lib
    COMMENT STLport
  )
  # ADD_DEPENDENCIES( depend stlp-depend )
endif (UNIX)

if (WIN32)
  ADD_CUSTOM_TARGET(
    release-shared ALL nmake /F msvc.mak INSTALL_LIB_DIR=${CMAKE_SOURCE_DIR}/lib install-release-shared
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/STLport/build/lib
    COMMENT STLport
  )
  ADD_CUSTOM_TARGET(
    depend ALL nmake /F msvc.mak INSTALL_LIB_DIR=${CMAKE_SOURCE_DIR}/lib depend
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/STLport/build/lib
    COMMENT STLport
  )
  # ADD_DEPENDENCIES( depend stlp-depend )
endif (WIN32)
