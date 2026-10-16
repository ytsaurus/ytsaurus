PY23_LIBRARY()


IF (OPENSOURCE)
    PY_SRCS(
        NAMESPACE yt.cron.library

        daemon_thread.py
        helpers.py
        juggler.py
        solomon.py
        token_variables.py
    )
    PEERDIR(
        contrib/python/simplejson
    )
ELSE()
    PY_SRCS(
        NAMESPACE yt.cron.library

        daemon_thread.py
        helpers.py
        juggler.py
        solomon.py
        token_variables.py

        helpers_yandex.py
    )

    PEERDIR(
        yp/python/client
        library/python/tvmauth
        contrib/python/simplejson
    )
ENDIF()

END()

RECURSE(
    logger
    orm
)
