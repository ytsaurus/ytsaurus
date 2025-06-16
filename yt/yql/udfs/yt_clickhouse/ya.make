IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")
    YQL_UDF(clickhouse_udf)

    YQL_ABI_VERSION(
        2
        41
        0
    )

    SRCS(
        yql/udfs/common/clickhouse/clickhouse_udf.cpp
        check_stack_size.cpp
    )

    PEERDIR(
        contrib/libs/apache/arrow
        yt/yt/library/clickhouse_functions
        contrib/clickhouse/src
        contrib/clickhouse/base/poco/Util
        library/cpp/yson/node
        yql/essentials/utils
        yql/essentials/public/udf/arrow
    )

    RESOURCE(
        yql/udfs/common/clickhouse/geo/regions_hierarchy.txt /geo/regions_hierarchy.txt
        yql/udfs/common/clickhouse/geo/regions_hierarchy_ua.txt /geo/regions_hierarchy_ua.txt
        yql/udfs/common/clickhouse/geo/regions_names_by.txt /geo/regions_names_by.txt
        yql/udfs/common/clickhouse/geo/regions_names_en.txt /geo/regions_names_en.txt
        yql/udfs/common/clickhouse/geo/regions_names_kz.txt /geo/regions_names_kz.txt
        yql/udfs/common/clickhouse/geo/regions_names_ru.txt /geo/regions_names_ru.txt
        yql/udfs/common/clickhouse/geo/regions_names_tr.txt /geo/regions_names_tr.txt
        yql/udfs/common/clickhouse/geo/regions_names_ua.txt /geo/regions_names_ua.txt
    )

    END()

ENDIF()
