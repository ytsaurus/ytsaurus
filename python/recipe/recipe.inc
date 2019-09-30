DEPENDS(${ARCADIA_ROOT}/yp/python/recipe)

INCLUDE(../../YaMakeBoilerplate.txt)

IF(YT_INSIDE_ARCADIA)
    USE_RECIPE(yp/python/recipe/recipe)
ELSE()
    USE_RECIPE(yp/python/recipe/recipe --outside-arcadia)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yp/python/ya_programs.make.inc)
