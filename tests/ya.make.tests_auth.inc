OWNER(g:yp)

SET(YP_TEST_SRCDIR yp/tests)

SET(YP_TEST_SRCS
    __init__.py
    conftest.py
    test_auth.py
    test_cypress_auth.py
    test_object_service.py
)

ENV(AUTH=1)

INCLUDE(../python/ya_programs.make.inc)

INCLUDE(${ARCADIA_ROOT}/library/testing/recipes/tvmtool/recipe.inc)
DATA(arcadia/yp/tests/data)
USE_RECIPE(
    library/testing/recipes/tvmtool/tvmtool yp/tests/data/tvmtool.conf
)

INCLUDE(${ARCADIA_ROOT}/yp/tests/blackbox_recipe/recipe.inc)
USE_RECIPE(
    yp/tests/blackbox_recipe/blackbox_recipe use_tvm
)

DEPENDS(
    ${YT_ROOT}/yt/packages/tests_package
)

PEERDIR(
    yp/python/lightweight_version_make
    yp/tests/helpers

    ${YT_PYTHON_ROOT}/python/yt/environment/arcadia_interop

    contrib/python/flaky
)

REQUIREMENTS(
    ram_disk:4
    cpu:4
)

SIZE(MEDIUM)

FORK_TESTS()
SPLIT_FACTOR(5)

NO_LINT()
