PACKAGE()

INCLUDE(${ARCADIA_ROOT}/yt/packages/ya.make.common)

# If you need to debug local changes, run tests with this flag set to true.
#
# E.g: ya make -DYT_RECIPE_BUILD_FROM_SOURCE=yes -r -tt yt/idm-integration/internal/app/gotest/
IF (NOT YT_RECIPE_BUILD_FROM_SOURCE)
    FROM_SANDBOX(
        FILE 5382815698
        OUT ytserver-all RENAME result/ytserver-all
        EXECUTABLE
    )

    FROM_SANDBOX(
        FILE 5382815920
        OUT yt_local RENAME result/yt_local
        EXECUTABLE
    )
ELSE()
    BUNDLE(yt/yt/server/all NAME ytserver-all)
    BUNDLE(yt/python/yt/local/bin/yt_local_make NAME yt_local)
ENDIF()

END()
