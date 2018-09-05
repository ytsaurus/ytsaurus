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
    "Choose the type of build, options are: None (CMAKE_CXX_FLAGS or CMAKE_C_FLAGS are used), Debug, Release, RelWithDebInfo."
    FORCE)
endif()

################################################################################
# Check available compiler version.

message(STATUS "Detected CXX compiler ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

set(_is_msvc FALSE)
set(_is_gcc FALSE)
set(_is_clang FALSE)

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  message(STATUS "Looks like we are using MSVC..." )

  if(MSVC_VERSION LESS 1800)
    message(FATAL_ERROR "You need at least Visual Studio 12 with Microsoft Visual C++ Compiler Nov 2013 CTP (CTP_Nov2013)")
  elseif(MSVC_VERSION EQUAL 1800)
    message(STATUS "Using Microsoft Visual Studio 2013 with Microsoft Visual C++ Compiler Nov 2013 CTP (CTP_Nov2013)")
    set(CMAKE_GENERATOR_TOOLSET "CTP_Nov2013" CACHE STRING "Platform Toolset" FORCE)
  endif()

  set(_is_msvc TRUE)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  message(STATUS "Looks like we are using gcc ${CMAKE_CXX_COMPILER_VERSION}...")

  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.9)
    message(FATAL_ERROR "g++ >= 4.9.0 is mandatory")
  endif()

  set(_is_gcc TRUE)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  message(STATUS "Looks like we are using clang...")

  # clang mimics GCC interface.
  set(_is_gcc TRUE)
  set(_is_clang TRUE)

  # COMPAT(sandello): We are not using this variable here, though external lists
  # may require this flag to tune compiler diagnostics.
  set(CMAKE_COMPILER_IS_GNUCXX TRUE)
  set(CMAKE_COMPILER_IS_CLANG TRUE)
endif()

################################################################################
# Configure compilation flags.

# First, provide an ability to specify custom comilation flags.
# Initially take these flags either from environment or from special variable.

set(CUSTOM_CMAKE_C_FLAGS
  $ENV{CFLAGS} ${USER_CMAKE_C_FLAGS} ${CUSTOM_CMAKE_C_FLAGS}
  CACHE STRING "User-defined C compiler flags")

set(CUSTOM_CMAKE_CXX_FLAGS
  $ENV{CXXFLAGS} ${USER_CMAKE_CXX_FLAGS} ${CUSTOM_CMAKE_CXX_FLAGS}
  CACHE STRING "User-defined C++ compiler flags")

# Now configure compiler options for g++ and clang.
if(_is_gcc)
  set(DIAGNOSTIC_FLAGS "-Wall -Werror")

  if(_is_clang)
    # Do not yell about unused arguments.
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Qunused-arguments")
    # Disable some annoying warnings.
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-c++1y-extensions")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-deprecated-register")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-deprecated-declarations")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-logical-op-parentheses")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-bitwise-op-parentheses")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-shift-op-parentheses")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-ignored-attributes")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-inconsistent-missing-override")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unused-const-variable")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unused-function")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unused-local-typedef")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-potentially-evaluated-expression")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unused-private-field")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unknown-warning-option")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-reserved-user-defined-literal")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-missing-braces")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-constant-logical-operand")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-enum-compare-switch")

    if(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
      set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unused-lambda-capture")
    endif()

    if(CMAKE_COLOR_MAKEFILE OR NOT DEFINED CMAKE_COLOR_MAKEFILE)
      set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -fcolor-diagnostics")
    endif()
  else()
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unused-function")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-maybe-uninitialized")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-sign-compare")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-parentheses")
    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -Wno-unused-local-typedefs")

    set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -frecord-gcc-switches")

    if(CMAKE_COLOR_MAKEFILE OR NOT DEFINED CMAKE_COLOR_MAKEFILE)
      set(DIAGNOSTIC_FLAGS "${DIAGNOSTIC_FLAGS} -fdiagnostics-color=always")
    endif()
  endif()

  if(CPU_VENDOR STREQUAL "GenuineIntel" OR CPU_VENDOR STREQUAL "AuthenticAMD")
    set(ARCH_FLAGS "-march=sandybridge -msse -msse2 -msse3")
    set(ARCH_FLAGS "${ARCH_FLAGS} -mno-avx -mpclmul")
  endif()

  if(YT_USE_SSE)
    set(ARCH_FLAGS "${ARCH_FLAGS} -msse4 -msse4.1 -msse4.2")
  endif()

  if(NOT _is_clang)
    # These are configuration-specific compilation flags.
    # http://gcc.gnu.org/onlinedocs/gcc/Option-Summary.html
    # Note that inlined version of memcmp is not used due to performance regressions in GCC.
    # http://gcc.gnu.org/bugzilla/show_bug.cgi?id=43052
    # http://gcc.gnu.org/onlinedocs/gcc-4.1.2/gcc/Other-Builtins.html
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-memcmp  -fno-builtin-memcpy  -fno-builtin-memset")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strcat  -fno-builtin-strchr  -fno-builtin-strcmp")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strcpy  -fno-builtin-strcspn -fno-builtin-strlen")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strncat -fno-builtin-strncmp -fno-builtin-strncpy")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strpbrk -fno-builtin-strrchr -fno-builtin-strspn")
    set(ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strstr")
  endif()

  set(CMAKE_C_FLAGS
    "-fPIC ${DIAGNOSTIC_FLAGS} ${ARCH_FLAGS} ${CUSTOM_CMAKE_C_FLAGS}"
    CACHE STRING "(Auto-generated) C compiler flags" FORCE)
  set(CMAKE_CXX_FLAGS
    "-std=c++1y -fPIC ${DIAGNOSTIC_FLAGS} ${ARCH_FLAGS} ${CUSTOM_CMAKE_CXX_FLAGS}"
    CACHE STRING "(Auto-generated) C++ compiler flags" FORCE)

  # Use libc++ on Mac OS X.
  if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -fvisibility-inlines-hidden")
  endif()

  if(YT_USE_LTO)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto")
  endif()

  set(_sanitize FALSE)

  if(YT_USE_ASAN)
    if(YT_ASAN_USE_DSO_RUNTIME)
      set(_shared_libasan_flag "-shared-libasan")
    else()
      set(_shared_libasan_flag "")
    endif()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -mllvm -asan-stack=0 ${_shared_libasan_flag}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -mllvm -asan-stack=0 ${_shared_libasan_flag}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -pie ${_shared_libasan_flag}")
    set(_sanitize TRUE)
  endif()

  if(YT_USE_TSAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread -pie")
    set(_sanitize TRUE)
  endif()

  if(YT_USE_MSAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=memory")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=memory")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory -pie")
    set(_sanitize TRUE)
  endif()

  if(_sanitize)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
  endif()

  set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fno-omit-frame-pointer" CACHE STRING "" FORCE)
  set(CMAKE_CXX_FLAGS_RELEASE "-O2" CACHE STRING "" FORCE)
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2" CACHE STRING "" FORCE)

  set(CMAKE_C_FLAGS_DEBUG "-g -O0 -fno-omit-frame-pointer" CACHE STRING "" FORCE)
  set(CMAKE_C_FLAGS_RELEASE "-O2" CACHE STRING "" FORCE)
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2" CACHE STRING "" FORCE)

  # TODO(sandello): Enable this when gcc will be stable.
  # set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-fwhole-program")
  # set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "-fwhole-program")

  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pthread")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -pthread")
  endif()
endif()

# Now configure compiler options for msvc.
if(_is_msvc)
  # These are default (basic) compilation flags.
  set(CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} /Gd /EHsc /bigobj")

  # These are configuration-specific compliation flags.
  # http://msdn.microsoft.com/en-us/library/fwkeyyhe.aspx
  set(CMAKE_CXX_FLAGS_DEBUG "/Zi /Od /Oy- /GS /MDd")
  set(CMAKE_CXX_FLAGS_RELEASE "/O2 /Oi /Oy- /GT")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "/Zi /O2 /Oi /Oy- /GT")

  set(CMAKE_C_FLAGS_DEBUG "/Zi /Od /Oy- /GS /MDd")
  set(CMAKE_C_FLAGS_RELEASE "/O2 /Oi /Oy- /GT")
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "/Zi /O2 /Oi /Oy- /GT")

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

if(_is_msvc)
else()
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_definitions(-DDEBUG)
  else()
    add_definitions(-DNDEBUG)
  endif()
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
