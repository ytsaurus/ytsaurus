GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/client/tests/ya.make.inc)

SRCS(${RPC_PROXY_CONNECTION_SHUTDOWN_SRCS})

USE_RECIPE(yt/yt/orm/example/python/recipe/bin/example-recipe)

END()
