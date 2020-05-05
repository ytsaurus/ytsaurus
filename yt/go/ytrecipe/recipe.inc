DEPENDS(
    yt/go/ytrecipe/cmd/jobrun
    yt/go/ytrecipe/cmd/ytrecipe
)

IF (NOT YTRECIPE_CONFIG_PATH)
    MESSAGE(FATAL_ERROR YTRECIPE_CONFIG_PATH variable is not set)
ENDIF()

DATA(arcadia/${YTRECIPE_CONFIG_PATH})

REQUIREMENTS(network:full)

USE_RECIPE(yt/go/ytrecipe/cmd/ytrecipe/ytrecipe
    --job-run yt/go/ytrecipe/cmd/jobrun/jobrun
    --config-path ${YTRECIPE_CONFIG_PATH})
