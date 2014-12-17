# Tunable parameters:
#   - ENV[CC]
#   - ENV[CXX]
#   - ENV[CFLAGS]
#   - ENV[CXXFLAGS]
#   - CUSTOM_CMAKE_C_FLAGS
#   - CUSTOM_CMAKE_CXX_FLAGS

################################################################################
# Enforce developer to specify build type.

if(CMAKE_BUILD_TYPE STREQUAL "")
  set(CMAKE_BUILD_TYPE "Debug"
    CACHE STRING
    "Choose the type of build, options are: None (CMAKE_CXX_FLAGS or CMAKE_C_FLAGS are used) Debug Release RelWithDebInfo MinSizeRel Sanitizer."
    FORCE)
endif()

################################################################################
# Check available compiler version.

if(MSVC OR MSVC_IDE)
  message(STATUS "Looks like we are using msvc..." )

  if(MSVC_VERSION LESS 1800)
    message(FATAL_ERROR "YT requires C++11. You need at least Visual Studio 12 with Microsoft Visual C++ Compiler Nov 2013 CTP (CTP_Nov2013).")
  elseif(MSVC_VERSION EQUAL 1800)
    message(STATUS "Using Microsoft Visual Studio 2013 with Microsoft Visual C++ Compiler Nov 2013 CTP (CTP_Nov2013)" )
    set(CMAKE_GENERATOR_TOOLSET "CTP_Nov2013" CACHE STRING "Platform Toolset" FORCE)
  endif()
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  message(STATUS "Looks like we are using gcc...")

  # XXX(psushin): Workaround for ccache.
  if(CMAKE_CXX_COMPILER_ARG1)
    string(REPLACE " " "" CMAKE_CXX_COMPILER_ARG1 ${CMAKE_CXX_COMPILER_ARG1})
    execute_process( 
      COMMAND ${CMAKE_CXX_COMPILER_ARG1} -dumpversion 
      OUTPUT_VARIABLE GCC_VERSION
    )
  else()
    execute_process(
      COMMAND ${CMAKE_CXX_COMPILER} -dumpversion
      OUTPUT_VARIABLE GCC_VERSION
    )
  endif()

  if(GCC_VERSION VERSION_LESS 4.7)
    message(FATAL_ERROR "g++ >= 4.7.0 is mandatory")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(STATUS "Looks like we are using clang...")

  # XXX(sandello): This is a temporary workaround.
  set(CMAKE_COMPILER_IS_GNUCXX TRUE)
  set(CMAKE_COMPILER_IS_CLANG TRUE)
endif()

################################################################################
# Configure compilation flags.

# First, provide an ability to specify custom comilation flags.
# Initially take these flags either from environment or from special variable.

set(CUSTOM_CMAKE_C_FLAGS
  $ENV{CFLAGS} ${USER_CMAKE_C_FLAGS}
  CACHE STRING "User-defined C compiler flags")

set(CUSTOM_CMAKE_CXX_FLAGS
  $ENV{CXXFLAGS} ${USER_CMAKE_CXX_FLAGS}
  CACHE STRING "User-defined C++ compiler flags")

# Now configure compiler options for g++ and clang.
if(CMAKE_COMPILER_IS_GNUCXX)
  set(DIAGNOSTIC_FLAGS "-Wall")

  if(CMAKE_COMPILER_IS_CLANG)
    # Do not yell about unused arguments.
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Qunused-arguments")
    # Disable some annoying warnings.
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-deprecated-register")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-logical-op-parentheses")
    if (CMAKE_COLOR_MAKEFILE OR NOT DEFINED CMAKE_COLOR_MAKEFILE)
      set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -fcolor-diagnostics")
    endif()
  else()
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-sign-compare -Wno-parentheses -Wno-unused-local-typedefs")
  endif()

  set(_vendor_id)
  set(_cpu_family)
  set(_cpu_model)
  set(_cpu_flags)

  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    file(READ "/proc/cpuinfo" _cpuinfo)
    string(REGEX REPLACE ".*vendor_id[ \t]*:[ \t]+([a-zA-Z0-9_-]+).*" "\\1" _vendor_id "${_cpuinfo}")
    string(REGEX REPLACE ".*cpu family[ \t]*:[ \t]+([a-zA-Z0-9_-]+).*" "\\1" _cpu_family "${_cpuinfo}")
    string(REGEX REPLACE ".*model[ \t]*:[ \t]+([a-zA-Z0-9_-]+).*" "\\1" _cpu_model "${_cpuinfo}")
    string(REGEX REPLACE ".*flags[ \t]*:[ \t]+([a-zA-Z0-9_-]+).*" "\\1" _cpu_flags "${_cpuinfo}")
 elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    get_filename_component(_vendor_id "[HKEY_LOCAL_MACHINE\\Hardware\\Description\\System\\CentralProcessor\\0;VendorIdentifier]" NAME CACHE)
    get_filename_component(_cpu_id "[HKEY_LOCAL_MACHINE\\Hardware\\Description\\System\\CentralProcessor\\0;Identifier]" NAME CACHE)
    mark_as_advanced(_vendor_id _cpu_id)
    string(REGEX REPLACE ".* Family ([0-9]+) .*" "\\1" _cpu_family "${_cpu_id}")
    string(REGEX REPLACE ".* Model ([0-9]+) .*" "\\1" _cpu_model "${_cpu_id}")
  endif(CMAKE_SYSTEM_NAME STREQUAL "Linux")

  if(NOT CMAKE_COMPILER_IS_CLANG)
    # These are configuration-specific compilation flags.
    # http://gcc.gnu.org/onlinedocs/gcc/Option-Summary.html
    # Note that inlined version of memcmp is not used due to performance regressions in GCC.
    # http://gcc.gnu.org/bugzilla/show_bug.cgi?id=43052
    # http://gcc.gnu.org/onlinedocs/gcc-4.1.2/gcc/Other-Builtins.html
    
    if(_vendor_id STREQUAL "GenuineIntel")
      set(ARCH_FLAGS "-march=native -msse -msse2 -msse3 -msse4 -msse4.1 -msse4.2 -mno-avx -mpclmul" )
    endif(_vendor_id STREQUAL "GenuineIntel")

    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-memcmp  -fno-builtin-memcpy  -fno-builtin-memset")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strcat  -fno-builtin-strchr  -fno-builtin-strcmp")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strcpy  -fno-builtin-strcspn -fno-builtin-strlen")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strncat -fno-builtin-strncmp -fno-builtin-strncpy")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strpbrk -fno-builtin-strrchr -fno-builtin-strspn")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strstr")
  endif()

  set(CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS} -fPIC ${DIAGNOSTIC_FLAGS} ${ARCH_FLAGS}"
    CACHE STRING "(Auto-generated) C compiler flags" FORCE)
  set(CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} -std=c++11 -fPIC ${DIAGNOSTIC_FLAGS} ${ARCH_FLAGS}"
    CACHE STRING "(Auto-generated) C++ compiler flags" FORCE)

  # Use libc++ on Mac OS X.
  if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
  endif()

  set(CMAKE_CXX_FLAGS_DEBUG "-g -O0"
    CACHE STRING "" FORCE)
  set(CMAKE_CXX_FLAGS_RELEASE "-O2 -flto"
    CACHE STRING "" FORCE)
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2 -flto"
    CACHE STRING "" FORCE)
  set(CMAKE_CXX_FLAGS_MINSIZEREL "-g -Os -flto"
    CACHE STRING "" FORCE)
  set(CMAKE_CXX_FLAGS_SANITIZER "-g -O1 -fsanitize=thread -fPIE"
    CACHE STRING "" FORCE)

  set(CMAKE_C_FLAGS_DEBUG "-g -O0"
    CACHE STRING "" FORCE)
  set(CMAKE_C_FLAGS_RELEASE "-O2 -flto"
    CACHE STRING "" FORCE)
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2 -flto"
    CACHE STRING "" FORCE)
  set(CMAKE_C_FLAGS_MINSIZEREL "-g -Os -flto"
    CACHE STRING "" FORCE)
  set(CMAKE_C_FLAGS_SANITIZER "-g -O1 -fsanitize=thread -fPIE"
    CACHE STRING "" FORCE)

  # TODO(sandello): Enable this when gcc will be stable.
  # set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-fwhole-program")
  # set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "-fwhole-program")
  set(CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "")
  set(CMAKE_EXE_LINKER_FLAGS_SANITIZER "-fsanitize=thread -pie")

  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pthread")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -pthread")
  endif()
endif()

# Now configure compiler options for msvc.
if(MSVC)
  # These are default (basic) compilation flags.
  set(CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} /Gd /EHsc /bigobj")

  # These are configuration-specific compliation flags.
  # http://msdn.microsoft.com/en-us/library/fwkeyyhe.aspx
  set(CMAKE_CXX_FLAGS_DEBUG "/Zi /Od /Oy- /GS /MDd")
  set(CMAKE_CXX_FLAGS_RELEASE "/O2 /Oi /Oy- /GT")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "/Zi /O2 /Oi /Oy- /GT")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "/O1 /Oi /Oy- /GT")

  set(CMAKE_C_FLAGS_DEBUG "/Zi /Od /Oy- /GS /MDd")
  set(CMAKE_C_FLAGS_RELEASE "/O2 /Oi /Oy- /GT")
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "/Zi /O2 /Oi /Oy- /GT")
  set(CMAKE_C_FLAGS_MINSIZEREL "/O1 /Oi /Oy- /GT")

  set(CMAKE_EXE_LINKER_FLAGS "/OPT:REF /OPT:ICF")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "/DEBUG")
  set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "/DEBUG")

  add_definitions(
    # For some reason MS wants to deprecate a bunch of standard functions
    -D_CRT_SECURE_NO_DEPRECATE
    -D_CRT_SECURE_NO_WARNINGS
    -D_CRT_NONSTDC_NO_DEPRECATE
    -D_CRT_NONSTDC_NO_WARNINGS
    -D_SCL_SECURE_NO_DEPRECATE
    -D_SCL_SECURE_NO_WARNINGS
  )
endif()

# Now configure platform-independent options.
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_definitions(-DDEBUG)
else()
  add_definitions(-DNDEBUG)
endif()

# Explicitly request C99 format macroses.
if(UNIX)
  add_definitions(-D_GNU_SOURCE)
  add_definitions(-D__STDC_CONSTANT_MACROS)
  add_definitions(-D__STDC_FORMAT_MACROS)
  add_definitions(-D__STDC_LIMIT_MACROS)
endif()

# On Apple we require some extra compatibility.
if(APPLE)
  add_definitions(-D_DARWIN_C_SOURCE)
endif()

#if(YT_WITH_VISIBILITY)
#  add_definitions(-fvisibility=hidden)
#  add_definitions(-fvisibility-inlines-hidden)
#  set(XCODE_ATTRIBUTE_GCC_SYMBOLS_PRIVATE_EXTERN "YES")
#  set(XCODE_ATTRIBUTE_GCC_INLINES_ARE_PRIVATE_EXTERN "YES")
#endif()

################################################################################
# Configure output paths.

if(UNIX)
  set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)
  set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
endif()

if(WIN32)
  set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
  set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
endif()

set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)
