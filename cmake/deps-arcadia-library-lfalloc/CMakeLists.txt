set(NAME deps-arcadia-library-lfalloc)
set(BASE ${CMAKE_SOURCE_DIR}/library/lfalloc)
set(SRCS
  ${BASE}/lf_allocX64.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_definitions(-DLFALLOC_YT)

add_library(${NAME} STATIC ${SRCS})

if(CMAKE_COMPILER_IS_GNUCXX)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -w)
endif()

target_link_libraries(${NAME} deps-arcadia-library-malloc-api)
