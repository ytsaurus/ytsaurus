IF (OS_LINUX AND CLANG AND NOT WITH_VALGRIND)

YQL_UDF_TEST()

DEPENDS(yql/udfs/common/clickhouse)

SIZE(MEDIUM)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()

ENDIF()
