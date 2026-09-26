LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

ADDINCL(
    # GLOBAL so consumers that include our headers (which pull in cppkafka -> librdkafka) resolve the
    # <librdkafka/rdkafka.h> prefix too.
    GLOBAL contrib/libs/librdkafka/include
)

SRCS(
    helpers.cpp
    kafka_client.cpp
    kafka_info.cpp
    read_session.cpp
    sink.cpp
    source.cpp
    spec.cpp
    GLOBAL register.cpp
)

PEERDIR(
    contrib/libs/cppkafka
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/resources
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(unittests)

# The integration tests need the Kafka broker recipe from library/recipes, unavailable in opensource.
IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(tests)
ENDIF()
