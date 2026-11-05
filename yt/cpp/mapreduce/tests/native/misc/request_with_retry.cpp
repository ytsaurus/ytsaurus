#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/retry_request.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/guid.h>
#include <util/generic/yexception.h>

using namespace NYT;
using namespace NYT::NTesting;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TAttempt
{
    // Mutation id passed by RequestWithRetry into the request.
    TMutationId Received;
    // Mutation id the request has actually sent (generated when the received one is empty).
    TMutationId Sent;
};

TVector<TAttempt> RunRequestFailingOnce(std::exception_ptr error)
{
    auto config = MakeIntrusive<TConfig>();
    config->RetryCount = 3;
    config->RetryInterval = TDuration::Zero();

    TVector<TAttempt> attempts;
    NYT::NDetail::RequestWithRetry<void>(
        CreateDefaultRequestRetryPolicy(config),
        [&] (TMutationId& mutationId) {
            auto& attempt = attempts.emplace_back();
            attempt.Received = mutationId;
            if (mutationId.IsEmpty()) {
                CreateGuid(&mutationId);
            }
            attempt.Sent = mutationId;
            if (attempts.size() == 1) {
                std::rethrow_exception(error);
            }
        });
    return attempts;
}

std::exception_ptr MakeErrorResponse(TYtError error)
{
    return std::make_exception_ptr(TErrorResponse(std::move(error), "test-request-id"));
}

void ExpectMutationIdKept(const TVector<TAttempt>& attempts)
{
    ASSERT_EQ(std::ssize(attempts), 2);
    EXPECT_TRUE(attempts[0].Received.IsEmpty());
    EXPECT_FALSE(attempts[0].Sent.IsEmpty());
    EXPECT_EQ(attempts[1].Received, attempts[0].Sent)
        << GetGuidAsString(attempts[1].Received) << " != " << GetGuidAsString(attempts[0].Sent);
    EXPECT_EQ(attempts[1].Sent, attempts[0].Sent);
}

void ExpectMutationIdReset(const TVector<TAttempt>& attempts)
{
    ASSERT_EQ(std::ssize(attempts), 2);
    EXPECT_TRUE(attempts[0].Received.IsEmpty());
    EXPECT_TRUE(attempts[1].Received.IsEmpty());
    EXPECT_NE(attempts[1].Sent, attempts[0].Sent)
        << GetGuidAsString(attempts[1].Sent) << " == " << GetGuidAsString(attempts[0].Sent);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

// RPC backend reports request timeouts as NYT::EErrorCode::Timeout.
TEST(RequestWithRetry, TimeoutKeepsMutationId)
{
    auto attempts = RunRequestFailingOnce(MakeErrorResponse(
        TYtError(NClusterErrorCodes::Timeout, "Request timed out")));
    ExpectMutationIdKept(attempts);
}

// Server-side timeout (e.g. proxy -> master) arrives as an inner error.
TEST(RequestWithRetry, NestedTimeoutKeepsMutationId)
{
    auto attempts = RunRequestFailingOnce(MakeErrorResponse(
        TYtError(NClusterErrorCodes::Generic, "Error executing request", {
            TYtError(NClusterErrorCodes::Timeout, "Request timed out"),
        })));
    ExpectMutationIdKept(attempts);
}

// HTTP backend wraps socket errors (including socket timeouts) as NBus::TransportError.
TEST(RequestWithRetry, TransportErrorKeepsMutationId)
{
    auto attempts = RunRequestFailingOnce(MakeErrorResponse(
        TYtError(NClusterErrorCodes::NBus::TransportError, "Request failed", {
            TYtError(NClusterErrorCodes::Generic, "Resource temporarily unavailable"),
        })));
    ExpectMutationIdKept(attempts);
}

TEST(RequestWithRetry, GenericExceptionKeepsMutationId)
{
    auto attempts = RunRequestFailingOnce(std::make_exception_ptr(yexception() << "Connection reset by peer"));
    ExpectMutationIdKept(attempts);
}

// The server has rejected the request without applying it, so the retry is a new mutation.
TEST(RequestWithRetry, UnavailableResetsMutationId)
{
    auto attempts = RunRequestFailingOnce(MakeErrorResponse(
        TYtError(NClusterErrorCodes::NRpc::Unavailable, "Service is unavailable")));
    ExpectMutationIdReset(attempts);
}

TEST(RequestWithRetry, RequestQueueSizeLimitExceededResetsMutationId)
{
    auto attempts = RunRequestFailingOnce(MakeErrorResponse(
        TYtError(NClusterErrorCodes::NRpc::RequestQueueSizeLimitExceeded, "Request queue size limit exceeded")));
    ExpectMutationIdReset(attempts);
}
