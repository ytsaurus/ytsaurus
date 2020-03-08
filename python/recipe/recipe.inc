DEPENDS(${ARCADIA_ROOT}/yp/python/recipe/bin)

INCLUDE(../../YaMakeBoilerplate.txt)

DEFAULT(YP_COUNT 1)

IF(YT_INSIDE_ARCADIA)
    USE_RECIPE(yp/python/recipe/bin/yp-recipe --count ${YP_COUNT})
ELSE()
    USE_RECIPE(yp/python/recipe/bin/yp-recipe --outside-arcadia --count ${YP_COUNT})
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yp/python/ya_programs.make.inc)
