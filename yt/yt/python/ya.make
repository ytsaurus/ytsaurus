RECURSE(
    yson
    yson_shared
    driver
    unittests
)
  
IF (NOT OPENSOURCE)
    RECURSE(
        yson_shared/py2
        yson_shared/py3
    )
ENDIF()
