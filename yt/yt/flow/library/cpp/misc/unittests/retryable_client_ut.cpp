#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NTableClient;
using namespace NQueryClient;
using namespace NYPath;

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrictMock;
using TStrictMockClient = StrictMock<NApi::TMockClient>;
using TStrictMockClientPtr = TIntrusivePtr<TStrictMockClient>;

using NTransactionClient::ITimestampProviderPtr;
using NTransactionClient::TTimestamp;

////////////////////////////////////////////////////////////////////////////////

using TStrictMockTimestampProvider = StrictMock<NTransactionClient::TMockTimestampProvider>;

TDynamicRetryableClientSpecPtr MakeShortRetrySpec()
{
    auto spec = New<TDynamicRetryableClientSpec>();
    spec->Timeout = TDuration::Seconds(10);
    spec->MinInnerTimeout = TDuration::MilliSeconds(10);
    spec->Backoff = TExponentialBackoffOptions{
        .InvocationCount = 100,
        .MinBackoff = TDuration::MilliSeconds(1),
        .MaxBackoff = TDuration::MilliSeconds(10),
        .BackoffMultiplier = 2.0,
    };
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRetryableClientTest, SimpleSuccess)
{
    auto rowset = CreateRowset<TUnversionedRow>(TNameTablePtr{}, {}); // Dummy rowset, need only unique address to compare later.
    auto client = New<TStrictMockClient>();
    EXPECT_CALL(*client, LookupRows(_, _, _, _))
        .WillOnce(testing::Invoke([&] () {
            return NYT::MakeFuture<TUnversionedLookupRowsResult>(TUnversionedLookupRowsResult{.Rowset = rowset});
        }));

    auto retryableClient = CreateRetryableClient(client, GetSyncInvoker(), CreateSyncStatusProfiler(), TLogger("test"));
    auto result = WaitFor(retryableClient->LookupRows("//path", {}, {}, {})).ValueOrThrow();
    EXPECT_EQ(result.Rowset, rowset);
}

TEST(TRetryableClientTest, RetryAndSuccess)
{
    auto rowset = CreateRowset<TUnversionedRow>(TNameTablePtr{}, {}); // Dummy rowset, need only unique address to compare later.
    auto client = New<TStrictMockClient>();
    EXPECT_CALL(*client, LookupRows(_, _, _, _))
        .WillOnce(testing::Invoke([&] () {
            return NYT::MakeFuture<TUnversionedLookupRowsResult>(TError(NYT::EErrorCode::Timeout, "Fake timeout"));
        }))
        .WillOnce(testing::Invoke([&] () {
            return NYT::MakeFuture<TUnversionedLookupRowsResult>(TUnversionedLookupRowsResult{.Rowset = rowset});
        }));

    auto retryableClient = CreateRetryableClient(client, GetSyncInvoker(), CreateSyncStatusProfiler(), TLogger("test"));
    auto result = WaitFor(retryableClient->LookupRows("//path", {}, {}, {})).ValueOrThrow();
    EXPECT_EQ(result.Rowset, rowset);
}

TEST(TRetryableClientTest, Timeout)
{
    auto aqueue = New<TActionQueue>();
    auto client = New<TStrictMockClient>();
    EXPECT_CALL(*client, LookupRows(_, _, _, _))
        .WillRepeatedly(testing::Invoke([&] () {
            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
            return NYT::MakeFuture<TUnversionedLookupRowsResult>(TError(NYT::EErrorCode::Timeout, "Fake timeout"));
        }));

    auto retryableClient = CreateRetryableClient(client, aqueue->GetInvoker(), CreateSyncStatusProfiler(), TLogger("test"));
    auto spec = New<TDynamicRetryableClientSpec>();
    spec->Timeout = TDuration::MilliSeconds(100);
    retryableClient->Reconfigure(spec);
    auto result = WaitFor(retryableClient->LookupRows("//path", {}, {}, {}));
    ASSERT_FALSE(result.IsOK());
    aqueue->Shutdown();
}

TEST(TRetryableClientTest, RetryableErrors)
{
    auto aqueue = New<TActionQueue>();
    auto client = New<TStrictMockClient>();
    EXPECT_CALL(*client, LookupRows(_, _, _, _))
        .WillRepeatedly(testing::Invoke([&] () {
            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
            return NYT::MakeFuture<TUnversionedLookupRowsResult>(TError(NYT::EErrorCode::Timeout, "Fake timeout"));
        }));

    auto statusProfiler = CreateSyncStatusProfiler();
    auto retryableClient = CreateRetryableClient(client, aqueue->GetInvoker(), statusProfiler, TLogger("test"));
    auto spec = New<TDynamicRetryableClientSpec>();
    retryableClient->Reconfigure(spec);
    auto future = retryableClient->LookupRows("//path", {}, {}, {});

    while (true) {
        auto errors = statusProfiler->GetStatus().Errors;
        for (const auto& [component, error] : errors) {
            Cerr << "Got: " << component << Endl;
            EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Timeout);
        }
        if (!errors.empty()) {
            break;
        }
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
    }

    future.Cancel(TError(NYT::EErrorCode::Canceled, "Canceled in test"));

    auto result = WaitFor(future);
    ASSERT_FALSE(result.IsOK());
    aqueue->Shutdown();
}

TEST(TRetryableClientTest, PrerequisiteCheckFailedIsNotRetriable)
{
    auto client = New<TStrictMockClient>();
    // StrictMock with a single expectation: a second (retry) call would fail the test.
    EXPECT_CALL(*client, LookupRows(_, _, _, _))
        .WillOnce(testing::Invoke([&] () {
            return NYT::MakeFuture<TUnversionedLookupRowsResult>(
                TError("Error committing transaction")
                << TError(NObjectClient::EErrorCode::PrerequisiteCheckFailed, "Prerequisite check failed"));
        }));

    auto retryableClient = CreateRetryableClient(client, GetSyncInvoker(), CreateSyncStatusProfiler(), TLogger("test"));
    auto result = WaitFor(retryableClient->LookupRows("//path", {}, {}, {}));
    ASSERT_FALSE(result.IsOK());
    EXPECT_TRUE(result.FindMatching(NObjectClient::EErrorCode::PrerequisiteCheckFailed).has_value());
}

////////////////////////////////////////////////////////////////////////////////

IRetryableClientPtr MakeRetryableClientOverTimestampProvider(
    const ITimestampProviderPtr& underlying,
    const IInvokerPtr& invoker,
    const IStatusProfilerPtr& statusProfiler,
    const TDynamicRetryableClientSpecPtr& spec)
{
    auto client = New<TStrictMockClient>();
    client->SetTimestampProvider(underlying);
    auto retryableClient = CreateRetryableClient(client, invoker, statusProfiler, TLogger("test"));
    retryableClient->Reconfigure(spec);
    return retryableClient;
}

TEST(TRetryableTimestampProviderTest, RetryAndSuccess)
{
    auto aqueue = New<TActionQueue>();
    auto underlying = New<TStrictMockTimestampProvider>();
    EXPECT_CALL(*underlying, GenerateTimestamps(_, _))
        .WillOnce(Return(MakeFuture<TTimestamp>(TError(NYT::EErrorCode::Timeout, "Fake timeout"))))
        .WillOnce(Return(MakeFuture<TTimestamp>(TError(NYT::EErrorCode::Timeout, "Fake timeout"))))
        .WillOnce(Return(MakeFuture<TTimestamp>(TTimestamp(42))));

    auto statusProfiler = CreateSyncStatusProfiler();
    auto retryableClient = MakeRetryableClientOverTimestampProvider(
        underlying,
        aqueue->GetInvoker(),
        statusProfiler,
        MakeShortRetrySpec());

    auto result = WaitFor(retryableClient->GetTimestampProvider()->GenerateTimestamps(/*count*/ 1, NObjectClient::InvalidCellTag)).ValueOrThrow();
    EXPECT_EQ(result, TTimestamp(42));
    aqueue->Shutdown();
}

TEST(TRetryableTimestampProviderTest, NonRetriableErrorPropagates)
{
    // StrictMock with a single expectation: a retry would be an unexpected second call and fail the test.
    auto underlying = New<TStrictMockTimestampProvider>();
    EXPECT_CALL(*underlying, GenerateTimestamps(_, _))
        .WillOnce(Return(MakeFuture<TTimestamp>(
            TError(NObjectClient::EErrorCode::PrerequisiteCheckFailed, "Prerequisite check failed"))));

    auto statusProfiler = CreateSyncStatusProfiler();
    auto retryableClient = MakeRetryableClientOverTimestampProvider(
        underlying,
        GetSyncInvoker(),
        statusProfiler,
        MakeShortRetrySpec());

    auto result = WaitFor(retryableClient->GetTimestampProvider()->GenerateTimestamps(/*count*/ 1, NObjectClient::InvalidCellTag));
    ASSERT_FALSE(result.IsOK());
    EXPECT_TRUE(result.FindMatching(NObjectClient::EErrorCode::PrerequisiteCheckFailed).has_value());
}

TEST(TRetryableTimestampProviderTest, BudgetExhausted)
{
    auto aqueue = New<TActionQueue>();
    auto underlying = New<TStrictMockTimestampProvider>();
    EXPECT_CALL(*underlying, GenerateTimestamps(_, _))
        .WillRepeatedly(Return(MakeFuture<TTimestamp>(TError(NYT::EErrorCode::Timeout, "Fake timeout"))));

    auto spec = New<TDynamicRetryableClientSpec>();
    spec->Timeout = TDuration::MilliSeconds(200);
    spec->MinInnerTimeout = TDuration::MilliSeconds(10);
    spec->Backoff = TExponentialBackoffOptions{
        .InvocationCount = 1000,
        .MinBackoff = TDuration::MilliSeconds(10),
        .MaxBackoff = TDuration::MilliSeconds(20),
        .BackoffMultiplier = 2.0,
    };

    auto statusProfiler = CreateSyncStatusProfiler();
    auto retryableClient = MakeRetryableClientOverTimestampProvider(
        underlying,
        aqueue->GetInvoker(),
        statusProfiler,
        spec);

    auto result = WaitFor(retryableClient->GetTimestampProvider()->GenerateTimestamps(/*count*/ 1, NObjectClient::InvalidCellTag));
    ASSERT_FALSE(result.IsOK());
    aqueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
