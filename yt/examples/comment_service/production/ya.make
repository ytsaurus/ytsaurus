PY3_PROGRAM()

PY_MAIN(main)

PEERDIR(
    yt/examples/comment_service/production/lib
)

PY_SRCS(
    TOP_LEVEL
    main.py
)

END()

RECURSE(lib)

IF (NOT OPENSOURCE)
    RECURSE(test)
ENDIF()
