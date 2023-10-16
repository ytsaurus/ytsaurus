#
# This file contains common sources for test_native_api/ and test_rpc_api/.
#
# NB. we don't make separate library from these files because gtest doesn't discover tests defined in libraries.
#

SET(THIS_DIR "${ARCADIA_ROOT}/yt/yt/tests/cpp")

SRCS(
    ${THIS_DIR}/test_type_v3.cpp
    ${THIS_DIR}/test_api.cpp
    ${THIS_DIR}/test_queues.cpp
    ${THIS_DIR}/modify_rows_test.cpp
)
