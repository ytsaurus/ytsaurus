PY3TEST()

# The trunk variant builds YT from source, which is prohibitively slow under a sanitizer build
# (everything is compiled in a single build). Skip it entirely when a sanitizer is enabled; the
# prebuilt-package variants still cover flow_execute under sanitizers.
IF (NOT SANITIZER_TYPE)
    # Build YT from source so this suite catches breakage on the YT side early — for any flow feature,
    # not a specific one (testing against trunk is how regressions are found fast).
    # This suite also exercises various pipeline interaction paths (HTTP proxy and CLI, besides the regular one).
    SET(YT_RECIPE_BUILD_FROM_SOURCE 1)

    INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/flow_execute/ya.make.inc)
ENDIF()

END()
