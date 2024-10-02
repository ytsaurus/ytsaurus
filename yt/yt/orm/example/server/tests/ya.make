GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

# Leak sanitizer triggers on some leaky singletons (e.g.: TJaegerTracer).
IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/python/ya_programs.make.inc)
INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_make_integration.inc)

DEPENDS(${ARCADIA_ROOT}/yt/yt/orm/example/python/recipe/bin)

DEFAULT(YT_CONFIG_YSON {"queue_agent_count"=1;})

USE_RECIPE(yt/yt/orm/example/python/recipe/bin/example-recipe --do-not-start-master --yt-config-yson ${YT_CONFIG_YSON})

SRCS(
    common.cpp
    test_aggregated_columns.cpp
    test_annotations_attribute.cpp
    test_attribute_type.cpp
    test_composite_key.cpp
    test_computed_fields_filter.cpp
    test_consumer.cpp
    test_execution_pool_tag.cpp
    test_meta_key.cpp
    test_misc.cpp
    test_one_to_many_attribute.cpp
    test_scalar_attribute.cpp
    test_view_value_getter.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/example/server/library

    yt/yt/core
    yt/yt/core/test_framework
)

IF (AUTOCHECK OR RELEASE_MACHINE OR YT_TEAMCITY)
    FORK_SUBTESTS(MODULO)
ENDIF()

END()
