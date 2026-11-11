DATA(
    arcadia/contrib/ydb/tests/supp
)

ENV(YDB_UBSAN_OPTIONS=contrib/ydb/tests/supp/ubsan_default.txt)

DEPENDS(
    contrib/ydb/tests/supp
)

USE_RECIPE(
    contrib/ydb/tests/supp/ydb_supp
)
