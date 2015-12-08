set(NAME ytext-arcadia-library-lfalloc)
set(BASE ${CMAKE_SOURCE_DIR}/library/lfalloc)
set(SRCS
  ${BASE}/lf_allocX64_old.cpp
)

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_definitions(-DLFALLOC_YT)

add_library(${NAME} STATIC ${SRCS})

if(CMAKE_COMPILER_IS_GNUCXX)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -w)
endif()

target_link_libraries(${NAME} ytext-arcadia-library-malloc-api)

