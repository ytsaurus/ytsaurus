LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

END()

RECURSE_FOR_TESTS(
    unittests
)
