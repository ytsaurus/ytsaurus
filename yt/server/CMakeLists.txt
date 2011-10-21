PROGRAM(server)

SRCS(
    stdafx.cpp
    main.cpp
    cell_master_server.cpp
    chunk_holder_server.cpp
)

PEERDIR(
    ADDINCL yt/ytlib
    dict/json
)

DEFAULT(INSTALLDIR ${INSTALL_BIN_DIR})
SET(INSTALLEDNAME ${PROG})

END()
