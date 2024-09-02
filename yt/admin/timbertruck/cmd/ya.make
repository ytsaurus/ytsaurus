RECURSE(
    timbertruck_os
)

IF (NOT OPENSOURCE)
    RECURSE(
        timbertruck_yandex
    )
ENDIF()
