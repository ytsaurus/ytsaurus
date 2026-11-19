#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/exceptions.h>

#include <netdb.h>

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

class TGetaddrinfoErrorCategory
    : public std::error_category
{
public:
    const char* name() const noexcept override
    {
        return "getaddrinfoError";
    }

    std::string message(int code) const override
    {
        return gai_strerror(code);
    }
};

const TGetaddrinfoErrorCategory GetaddrinfoErrorCategory;

clickhouse::ServerException MakeServerException(int code)
{
    return clickhouse::ServerException(std::make_shared<clickhouse::Exception>(
        clickhouse::Exception{.code = code, .display_text = "server error"}));
}

TEST(TClassifyClickHouseAdmissionErrorTest, VerifiedTransportFailuresAreUnavailable)
{
    for (auto code : {
            std::errc::connection_refused,
            std::errc::connection_reset,
            std::errc::connection_aborted,
            std::errc::timed_out,
            std::errc::host_unreachable,
            std::errc::network_unreachable,
            std::errc::network_down,
            std::errc::not_connected,
            std::errc::broken_pipe,
            std::errc::resource_unavailable_try_again,
         })
    {
        SCOPED_TRACE(std::make_error_code(code).message());
        EXPECT_EQ(
            ClassifyClickHouseAdmissionError(std::system_error(std::make_error_code(code), "fail to connect")),
            EClickHouseAdmissionFailureKind::Unavailable);
        EXPECT_EQ(
            ClassifyClickHouseAdmissionError(std::system_error(static_cast<int>(code), std::system_category())),
            EClickHouseAdmissionFailureKind::Unavailable);
    }
    for (int code : {EAI_AGAIN, EAI_NONAME}) {
        EXPECT_EQ(
            ClassifyClickHouseAdmissionError(std::system_error(code, GetaddrinfoErrorCategory)),
            EClickHouseAdmissionFailureKind::Unavailable);
    }
}

TEST(TClassifyClickHouseAdmissionErrorTest, UnverifiedFailuresAreIndeterminate)
{
    EXPECT_EQ(
        ClassifyClickHouseAdmissionError(std::system_error(std::make_error_code(std::errc::connection_reset), "closed")),
        EClickHouseAdmissionFailureKind::Indeterminate);
    EXPECT_EQ(
        ClassifyClickHouseAdmissionError(std::system_error(std::make_error_code(std::errc::invalid_argument))),
        EClickHouseAdmissionFailureKind::Indeterminate);
    EXPECT_EQ(
        ClassifyClickHouseAdmissionError(std::system_error(EAI_FAIL, GetaddrinfoErrorCategory)),
        EClickHouseAdmissionFailureKind::Indeterminate);
    EXPECT_EQ(
        ClassifyClickHouseAdmissionError(clickhouse::ProtocolError("can't read data packet from input stream")),
        EClickHouseAdmissionFailureKind::Indeterminate);
    EXPECT_EQ(
        ClassifyClickHouseAdmissionError(clickhouse::OpenSSLError("OpenSSL error: 5 : unexpected eof")),
        EClickHouseAdmissionFailureKind::Indeterminate);
    EXPECT_EQ(ClassifyClickHouseAdmissionError(MakeServerException(999)), EClickHouseAdmissionFailureKind::Indeterminate);
    EXPECT_EQ(ClassifyClickHouseAdmissionError(std::runtime_error("misc")), EClickHouseAdmissionFailureKind::Indeterminate);
    EXPECT_EQ(
        ClassifyClickHouseAdmissionError(TErrorException() <<= TError("Unexpected ClickHouse metadata result column count")),
        EClickHouseAdmissionFailureKind::Indeterminate);
}

TEST(TClassifyClickHouseAdmissionErrorTest, DefinitiveFailuresAreIncompatible)
{
    for (int code : {60, 81, 192, 193, 194, 195, 497, 516}) {
        SCOPED_TRACE(code);
        EXPECT_EQ(ClassifyClickHouseAdmissionError(MakeServerException(code)), EClickHouseAdmissionFailureKind::Incompatible);
    }
    EXPECT_EQ(
        ClassifyClickHouseAdmissionError(clickhouse::OpenSSLError("Failed to verify SSL connection, X509_v error: 18")),
        EClickHouseAdmissionFailureKind::Incompatible);
}

TEST(TClassifyClickHouseAdmissionErrorTest, TypedOutcomeSurvivesWrapping)
{
    for (auto kind : TEnumTraits<EClickHouseAdmissionFailureKind>::GetDomainValues()) {
        EXPECT_EQ(
            ClassifyClickHouseAdmissionError(TClickHouseAdmissionException(kind, TError("wrapped"))),
            kind);
    }
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

} // namespace
} // namespace NYT::NFlow
