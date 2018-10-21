include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

add_executable(unittester-python
  ${YT_SRCS_RESOLVED}
)
  
#get_python_kind_specific_constants(python2.7 PYTHON_KIND_DASHED TARGET_NAME_PREFIX)

target_include_directories(unittester-python
  PUBLIC ${PYCXX_BASE} ${PYTHON_2_7_INCLUDE_DIR}
)

target_link_libraries(unittester-python
  yt-core-test-framework
  yt-core
  yt-core-alloc
  yt-library-ytlib
  yt-python-2-7-common
  yt-python-2-7-pycxx
  python2.7
)
