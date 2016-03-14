OWNER(
    exprmntr
    vartyukh
)

UNION()

BUILD_ONLY_IF(LINUX)

FILES(
    __init__.py
    yt_stuff.py
)

#IF ()
    FROM_SANDBOX(FILE 112204044 OUT yt.tgz)
#ELSEIF()
#ELSE()
#ENDIF()

END()

RECURSE(
    test
)
