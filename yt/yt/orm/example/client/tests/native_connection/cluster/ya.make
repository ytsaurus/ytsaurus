GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SET(EXAMPLE_MASTER_COUNT 3)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/client/tests/ya.make.inc)

SRCS(${NATIVE_CONNECTION_CLUSTER_SRCS})

USE_RECIPE(
    yt/yt/orm/example/python/recipe/bin/example-recipe
    --use-native-connection
    --master-count=${EXAMPLE_MASTER_COUNT}
)

END()
