NO_BUILD_IF(SANITIZER_TYPE)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

RECURSE(
    cmd
    internal
)
