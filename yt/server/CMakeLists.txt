add_executable( server
    stdafx.cpp
    main.cpp
    cell_master_server.cpp
    chunk_holder_server.cpp
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server
  ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree
)

#PEERDIR(
#    ADDINCL yt/ytlib
#    dict/json
#)

#DEFAULT(INSTALLDIR ${INSTALL_BIN_DIR})
#SET(INSTALLEDNAME ${PROG})

if (CMAKE_COMPILER_IS_GNUCXX)
  SET( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -nodefaultlibs -L${CMAKE_BINARY_DIR}/lib -Wl,-rpath=${CMAKE_BINARY_DIR}/lib" )

  target_link_libraries( server
    #-lprotoc_
    #-lprotobuf
    -lytlib
    -larcadia-utils
    -lsnappy
    -llz4
    -lfastlz
    -lminilzo
    -larczlib
    -lprotobuf
    -lqmisc
    -lNetLiba
    # -lyajl
    -ljson
    -lstlport
  )
  target_link_libraries( server -larcadia-utils )

  target_link_libraries( server -lc -lm -lpthread -ldl -lgcc_s )
endif()

#if (NOT WIN32)
#  INSTALL(TARGETS server RUNTIME DESTINATION ${CMAKE_BINARY_DIR}/bin)
#endif()
