IF(OS_LINUX)
    PY3TEST()
    TEST_SRCS(test.py)

    DEPENDS(
        contrib/ydb/library/yql/tools/mrjob
    )

    END()
ENDIF()
