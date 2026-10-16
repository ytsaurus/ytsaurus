#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/exceptions.h>

#include <system_error>

namespace NYT::NFlow {
namespace {

TEST(TClassifyClickHouseErrorTest, Permanent)
{
    EXPECT_EQ(ClassifyClickHouseError(clickhouse::ValidationError("bad column")), EClickHouseErrorKind::Permanent);
    EXPECT_EQ(ClassifyClickHouseError(clickhouse::UnimplementedError("unsupported")), EClickHouseErrorKind::Permanent);
    EXPECT_EQ(ClassifyClickHouseError(clickhouse::AssertionError("internal")), EClickHouseErrorKind::Permanent);
}

TEST(TClassifyClickHouseErrorTest, Retryable)
{
    EXPECT_EQ(ClassifyClickHouseError(clickhouse::ProtocolError("truncated packet")), EClickHouseErrorKind::Retryable);
    EXPECT_EQ(ClassifyClickHouseError(clickhouse::OpenSSLError("handshake failed")), EClickHouseErrorKind::Retryable);
    EXPECT_EQ(
        ClassifyClickHouseError(std::system_error(std::make_error_code(std::errc::connection_refused))),
        EClickHouseErrorKind::Retryable);
}

TEST(TClassifyClickHouseErrorTest, Unclassified)
{
    auto serverException = clickhouse::ServerException(std::make_shared<clickhouse::Exception>(
        clickhouse::Exception{.code = 469, .display_text = "constraint violated"}));
    EXPECT_EQ(ClassifyClickHouseError(serverException), EClickHouseErrorKind::Unclassified);
    EXPECT_EQ(ClassifyClickHouseError(clickhouse::CompressionError("checksum mismatch")), EClickHouseErrorKind::Unclassified);
    EXPECT_EQ(ClassifyClickHouseError(std::runtime_error("misc")), EClickHouseErrorKind::Unclassified);
}

TEST(TValidateTargetEngineTest, FailoverMatrix)
{
    const NLogging::TLogger logger("Test");
    EXPECT_NO_THROW(ValidateTargetEngine(
        "ReplicatedMergeTree",
        "db",
        "events",
        false,
        logger));
    EXPECT_NO_THROW(ValidateTargetEngine(
        "ReplicatedMergeTree",
        "db",
        "events",
        true,
        logger));
    EXPECT_NO_THROW(ValidateTargetEngine(
        "SharedMergeTree",
        "db",
        "events",
        false,
        logger));
    EXPECT_THROW(
        ValidateTargetEngine(
            "SharedMergeTree",
            "db",
            "events",
            true,
            logger),
        std::exception);
    EXPECT_NO_THROW(ValidateTargetEngine(
        "MergeTree",
        "db",
        "events",
        false,
        logger));
    EXPECT_THROW(
        ValidateTargetEngine(
            "MergeTree",
            "db",
            "events",
            true,
            logger),
        std::exception);
    EXPECT_THROW(
        ValidateTargetEngine(
            "Distributed",
            "db",
            "events",
            false,
            logger),
        std::exception);
    EXPECT_THROW(
        ValidateTargetEngine(
            "Log",
            "db",
            "events",
            false,
            logger),
        std::exception);
}

TEST(TClickHouseFailureActionTest, ConfigurationFailureIsNeverAcknowledged)
{
    const TClickHouseRequestOptions requestOptions{
        .AsyncInsert = false,
        .MaxInsertAttempts = 1,
    };
    TClickHouseAttemptState attemptState;

    EXPECT_EQ(
        GetClickHouseFailureAction(
            EWriteGuarantee::AtMostOnce,
            /*insertStarted*/ false,
            EClickHouseErrorKind::Unclassified,
            requestOptions,
            &attemptState),
        EClickHouseFailureAction::Retry);
    EXPECT_EQ(attemptState.UnclassifiedInsertAttempts, 0);
}

} // namespace
} // namespace NYT::NFlow
