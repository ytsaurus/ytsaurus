if (UNIX)
  set( LTYPE SHARED )
endif()

if (WIN32)
  set( LTYPE STATIC )
endif()

set( BASE ${CMAKE_SOURCE_DIR}/quality/Misc )

add_library( ytext-quality-misc ${LTYPE}
  ${BASE}/BasicShare.cpp
  ${BASE}/FastIO.cpp
  ${BASE}/HPTimer.cpp
  ${BASE}/Guid.cpp
  ${BASE}/MemIO.cpp
  ${BASE}/TxtSaver.cpp
  ${BASE}/LockFreeQueue.cpp
  ${BASE}/matrix.cpp
  ${BASE}/job_queue.cpp
  ${BASE}/jenkinsHash.cpp
  ${BASE}/thrref.h
  ${BASE}/safe_serialize.h
  ${BASE}/fmt_reader.h
  ${BASE}/freq_substr.cpp
  ${BASE}/shmat.cpp
  ${BASE}/StdAfx.cpp
  ${CMAKE_SOURCE_DIR}/library/simplehashtrie/hash_trie.cpp
)

target_link_libraries( ytext-quality-misc
  ytext-arcadia-util
)

include_directories(
  ${CMAKE_SOURCE_DIR}
)

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( ytext-quality-misc stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( ytext-quality-misc PROPERTIES LINK_FLAGS "-nodefaultlibs" )
  endif()
endif()

install(
  TARGETS ytext-quality-misc
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
