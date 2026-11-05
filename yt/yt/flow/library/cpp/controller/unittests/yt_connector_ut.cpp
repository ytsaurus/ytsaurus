#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/controller/private.h>

#include <yt/yt/core/bus/public.h>

namespace NYT::NFlow::NController {
namespace {

////////////////////////////////////////////////////////////////////////////////

const std::string ControllerAddress = "[2a02:6b8::1]:1234";

TError MakeConfirmationError(TErrorCode code, const std::string& address)
{
    return TError("Internal RPC call failed")
        .With(TError(code, "Failed to establish TLS/SSL session")
                .With("address", address));
}

TError MakeSslError(const std::string& address)
{
    return MakeConfirmationError(NBus::EErrorCode::SslError, address);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClassifyLeaderConfirmationTest, SkippedByEnvironment)
{
    EXPECT_EQ(
        ClassifyLeaderConfirmation(
            /*skipConfirmationFromEnv*/ true,
            TError(),
            ControllerAddress,
            /*busServerHasTlsMaterial*/ false),
        ELeaderConfirmationResult::SkippedByEnvironment);
}

TEST(TClassifyLeaderConfirmationTest, Confirmed)
{
    EXPECT_EQ(
        ClassifyLeaderConfirmation(
            /*skipConfirmationFromEnv*/ false,
            TError(),
            ControllerAddress,
            /*busServerHasTlsMaterial*/ false),
        ELeaderConfirmationResult::Confirmed);
}

TEST(TClassifyLeaderConfirmationTest, SslErrorWithoutTlsMaterial)
{
    EXPECT_EQ(
        ClassifyLeaderConfirmation(
            /*skipConfirmationFromEnv*/ false,
            MakeSslError(ControllerAddress),
            ControllerAddress,
            /*busServerHasTlsMaterial*/ false),
        ELeaderConfirmationResult::SkippedWithoutTlsMaterial);
}

TEST(TClassifyLeaderConfirmationTest, SslErrorWithTlsMaterial)
{
    EXPECT_EQ(
        ClassifyLeaderConfirmation(
            /*skipConfirmationFromEnv*/ false,
            MakeSslError(ControllerAddress),
            ControllerAddress,
            /*busServerHasTlsMaterial*/ true),
        ELeaderConfirmationResult::Failed);
}

TEST(TClassifyLeaderConfirmationTest, SslErrorForAnotherAddress)
{
    EXPECT_EQ(
        ClassifyLeaderConfirmation(
            /*skipConfirmationFromEnv*/ false,
            MakeSslError("[2a02:6b8::2]:1234"),
            ControllerAddress,
            /*busServerHasTlsMaterial*/ false),
        ELeaderConfirmationResult::Failed);
}

TEST(TClassifyLeaderConfirmationTest, NonSslError)
{
    EXPECT_EQ(
        ClassifyLeaderConfirmation(
            /*skipConfirmationFromEnv*/ false,
            MakeConfirmationError(NBus::EErrorCode::TransportError, ControllerAddress),
            ControllerAddress,
            /*busServerHasTlsMaterial*/ false),
        ELeaderConfirmationResult::Failed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
