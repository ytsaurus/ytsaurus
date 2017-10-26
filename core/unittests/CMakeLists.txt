include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

add_executable(unittester-core
  ${YT_SRCS_RESOLVED}
)

if (_is_clang OR (_is_gcc AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 6.0))
  set_source_files_properties(${CMAKE_CURRENT_LIST_DIR}/enum_ut.cpp PROPERTIES COMPILE_FLAGS -Wno-tautological-compare)
endif()

target_link_libraries(unittester-core
  yt-core-test-framework
  yt-core
  yt-core-grpc
  yt-core-crypto
)
