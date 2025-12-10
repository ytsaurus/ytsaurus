PY3TEST()

TEST_SRCS(
    test_resource_usage.py
)

SIZE(LARGE)
TAG(ya:fat)

PEERDIR(
    yt/python/yt/wrapper/testlib
    yt/yt/tests/library
)

DEPENDS(
    yt/microservices/resource_usage_roren
    yt/microservices/resource_usage/json_api_go/cmd/resource_usage_api
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

TIMEOUT(3600)

REQUIREMENTS(
    cpu:16
    ram:64
    ram_disk:32
)

END()
