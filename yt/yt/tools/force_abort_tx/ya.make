PROGRAM()

SRCS(
    main.cpp
)

IF (OPENSOURCE)
    SRCS(secrets_os.cpp)
ELSE()
    SRCS(secrets_yandex.cpp)
ENDIF()

PEERDIR(
    yt/yt/ytlib
)

END()
