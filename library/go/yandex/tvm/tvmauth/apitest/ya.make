GO_TEST()

SET(GOEXPERIMENT cgocheck2)

INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmapi/recipe.inc)

GO_TEST_SRCS(client_test.go)

END()
