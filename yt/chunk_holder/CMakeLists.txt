PROGRAM(yt)

SRCS(
	main.cpp
)

PEERDIR(
    ADDINCL junk/monster/yt/ytlib
    quality/util
    dict/json
)

DEFAULT(INSTALLDIR ${INSTALL_BIN_DIR})
SET(INSTALLEDNAME ${PROG})

END()
