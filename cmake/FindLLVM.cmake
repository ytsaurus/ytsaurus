#  Find the native LLVM includes and libraries
#
#  LLVM_VERSION      - LLVM version.
#  LLVM_INCLUDE_DIRS - Directory containing LLVM headers.
#  LLVM_LIBRARY_DIRS - Directory containing LLVM libraries.
#  LLVM_CPPFLAGS     - C preprocessor flags for files that include LLVM headers.
#  LLVM_CXXFLAGS     - C++ compiler flags for files that include LLVM headers.
#  LLVM_LDFLAGS      - Linker flags.
#  LLVM_FOUND        - True if LLVM found.

#  llvm_map_components_to_libraries - Maps LLVM used components to required libraries.
#  Usage: llvm_map_components_to_libraries(REQUIRED_LLVM_LIBRARIES core jit interpreter native ...)

# First look in ENV{LLVM_ROOT} then system path.
find_program(LLVM_CONFIG_EXECUTABLE llvm-config-3.5 llvm-config
  PATHS
  $ENV{LLVM_ROOT}/bin
)

if (NOT LLVM_CONFIG_EXECUTABLE)
  message(FATAL_ERROR "LLVM package can't be found (could not find llvm-config). Set environment variable LLVM_ROOT.")
else()
  SET(LLVM_FOUND TRUE)
  
  execute_process(
    COMMAND ${LLVM_CONFIG_EXECUTABLE} --version
    OUTPUT_VARIABLE LLVM_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  
  if (NOT ${LLVM_VERSION} MATCHES "3.5")
    message(FATAL_ERROR "Required LLVM version is 3.5")
  endif()
  
  execute_process(
    COMMAND ${LLVM_CONFIG_EXECUTABLE} --includedir
    OUTPUT_VARIABLE LLVM_INCLUDE_DIRS
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  
  execute_process(
    COMMAND ${LLVM_CONFIG_EXECUTABLE} --libdir
    OUTPUT_VARIABLE LLVM_LIBRARY_DIRS
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  
  execute_process(
    COMMAND ${LLVM_CONFIG_EXECUTABLE} --cppflags
    OUTPUT_VARIABLE LLVM_CPPFLAGS
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  
  execute_process(
    COMMAND ${LLVM_CONFIG_EXECUTABLE} --cxxflags
    OUTPUT_VARIABLE LLVM_CXXFLAGS
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  
  execute_process(
    COMMAND ${LLVM_CONFIG_EXECUTABLE} --ldflags
    OUTPUT_VARIABLE LLVM_LDFLAGS
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  
  # Get the link libs we need.  llvm has many and we don't want to link all of the libs
  # if we don't need them.   
  function(llvm_map_components_to_libraries OUT_VAR)
    

    if (MSVC OR MSVC_IDE)
      # Workarounds
      execute_process(
        COMMAND ${LLVM_CONFIG_EXECUTABLE} --libs ${ARGN}
        OUTPUT_VARIABLE LLVM_MODULE_LIBS
        OUTPUT_STRIP_TRAILING_WHITESPACE
      )

      #string(REGEX REPLACE ".a " ".lib " LLVM_MODULE_LIBS "${LLVM_MODULE_LIBS}")
      string(REGEX REPLACE "-l" "" LLVM_MODULE_LIBS "${LLVM_MODULE_LIBS}")
      string(REPLACE " " ";" LIBS_LIST "${LLVM_MODULE_LIBS}")
      set (LLVM_MODULE_LIBS "")
      foreach (LIB ${LIBS_LIST})
        set(LLVM_MODULE_LIBS ${LLVM_MODULE_LIBS} "${LLVM_LIBRARY_DIRS}/${LIB}.lib")
      endforeach(LIB)
    else()
      execute_process(
        COMMAND ${LLVM_CONFIG_EXECUTABLE} --libfiles ${ARGN}
        OUTPUT_VARIABLE LLVM_MODULE_LIBS
        OUTPUT_STRIP_TRAILING_WHITESPACE
      )
      string(REPLACE " " ";" LIBS_LIST "${LLVM_MODULE_LIBS}")
    endif()

    message(STATUS "LLVM libs: ${LLVM_MODULE_LIBS}")
  
    execute_process(
      COMMAND ${LLVM_CONFIG_EXECUTABLE} --system-libs ${ARGN}
      OUTPUT_VARIABLE LLVM_MODULE_SYSTEM_LIBS
      OUTPUT_STRIP_TRAILING_WHITESPACE
    )
  
    string(REPLACE "\n" "" LLVM_MODULE_SYSTEM_LIBS "${LLVM_MODULE_SYSTEM_LIBS}")
    string(REPLACE " " ";" SYSTEM_LIBS_LIST "${LLVM_MODULE_SYSTEM_LIBS}")

    set( ${OUT_VAR} ${SYSTEM_LIBS_LIST} ${LIBS_LIST} PARENT_SCOPE )
  endfunction(llvm_map_components_to_libraries)
  
  message(STATUS "LLVM include dir: ${LLVM_INCLUDE_DIRS}")
  message(STATUS "LLVM lib dir: ${LLVM_LIBRARY_DIRS}")
  message(STATUS "LLVM C preprocessor: ${LLVM_CPPFLAGS}")
  message(STATUS "LLVM C++ compiler: ${LLVM_CXXFLAGS}")

endif (NOT LLVM_CONFIG_EXECUTABLE)



