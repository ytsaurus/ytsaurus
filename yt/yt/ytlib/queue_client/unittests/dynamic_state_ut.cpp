#include <yt/yt/ytlib/queue_client/config.h>
#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NQueueClient {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;

using ::testing::_;
using ::testing::AnyNumber;

// NB(apachee): The counters are only read after the corresponding fiber has finished,
// so there is no need for them to be atomic.
using TCallCounterPtr = std::shared_ptr<int>;

////////////////////////////////////////////////////////////////////////////////

TExponentialBackoffOptions MakeTestRetryBackoffOptions(int retryCount)
{
    return TExponentialBackoffOptions{
        .InvocationCount = retryCount,
        .MinBackoff = TDuration::MilliSeconds(1),
        .MaxBackoff = TDuration::MilliSeconds(1),
        .BackoffJitter = 0.0,
    };
}

TSelectRowsResult MakeEmptySelectResult()
{
    return TSelectRowsResult{
        .Rowset = CreateRowset(
            NRecords::TQueueObjectDescriptor::Get()->GetNameTable(),
            TSharedRange<TUnversionedRow>()),
    };
}

std::vector<TQueueTableRow> MakeQueueTableRows()
{
    NYPath::TRichYPath path("//tmp/queue");
    path.SetCluster("test_cluster");

    return {TQueueTableRow{.Path = TTablePath(path)}};
}

//! Runs #callback in a fiber, so that the code under test has a proper current invoker,
//! and returns whatever error it has thrown, if any.
template <class TCallbackType>
TError RunInFiber(const TCallbackType& callback)
{
    auto actionQueue = New<TActionQueue>("DynamicStateTest");
    auto result = callback
        .AsyncVia(actionQueue->GetInvoker())
        .Run()
        .BlockingGet();
    actionQueue->Shutdown(/*graceful*/ true);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TStateTableRetriesTest
    : public ::testing::Test
{
protected:
    TIntrusivePtr<TMockClient> Client_ = New<TMockClient>();

    TQueueTablePtr CreateTable()
    {
        return New<TQueueTable>("//tmp/queue_agent", Client_);
    }

    TQueueTablePtr CreateTable(const TExponentialBackoffOptions& retryBackoffOptions)
    {
        return New<TQueueTable>("//tmp/queue_agent", Client_, retryBackoffOptions);
    }

    //! Makes #SelectRows fail #failureCount times in a row and succeed afterwards.
    //! The returned counter holds the total number of performed calls.
    TCallCounterPtr MockFailingSelectRows(int failureCount)
    {
        auto callCount = std::make_shared<int>(0);
        EXPECT_CALL(*Client_, SelectRows(_, _))
            .WillRepeatedly(::testing::Invoke([callCount, failureCount] (
                const std::string& /*query*/,
                const TSelectRowsOptions& /*options*/)
            {
                if (++*callCount <= failureCount) {
                    return MakeFuture<TSelectRowsResult>(TError("Transient failure"));
                }
                return MakeFuture(MakeEmptySelectResult());
            }));
        return callCount;
    }

    //! Makes #StartTransaction return a transaction whose commit fails #failureCount times
    //! in a row and succeeds afterwards. The returned counter holds the total number of commits.
    TCallCounterPtr MockFailingCommit(int failureCount)
    {
        auto transaction = New<TMockTransaction>();
        auto commitCount = std::make_shared<int>(0);

        EXPECT_CALL(*transaction, WriteRows(_, _, _, _, _))
            .Times(AnyNumber());
        EXPECT_CALL(*transaction, DeleteRows(_, _, _, _))
            .Times(AnyNumber());
        EXPECT_CALL(*transaction, Commit(_))
            .WillRepeatedly(::testing::Invoke([commitCount, failureCount] (
                const TTransactionCommitOptions& /*options*/)
            {
                if (++*commitCount <= failureCount) {
                    return MakeFuture<TTransactionCommitResult>(TError("Transient commit failure"));
                }
                return MakeFuture(TTransactionCommitResult{});
            }));

        EXPECT_CALL(*Client_, StartTransaction(_, _))
            .WillRepeatedly(::testing::Return(MakeFuture<ITransactionPtr>(transaction)));

        return commitCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStateTableRetriesTest, NoRetriesByDefault)
{
    auto callCount = MockFailingSelectRows(/*failureCount*/ 1);
    auto table = CreateTable();

    auto error = RunInFiber(BIND([&] {
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(table->Select()).ThrowOnError(),
            "Transient failure");
    }));

    EXPECT_TRUE(error.IsOK())
        << ToString(error);
    EXPECT_EQ(1, *callCount);
}

TEST_F(TStateTableRetriesTest, RetriedUntilSuccess)
{
    auto callCount = MockFailingSelectRows(/*failureCount*/ 2);
    auto table = CreateTable(MakeTestRetryBackoffOptions(/*retryCount*/ 3));

    auto error = RunInFiber(BIND([&] {
        EXPECT_NO_THROW(WaitFor(table->Select()).ThrowOnError());
    }));

    EXPECT_TRUE(error.IsOK())
        << ToString(error);
    EXPECT_EQ(3, *callCount);
}

TEST_F(TStateTableRetriesTest, AttemptsAreLimited)
{
    auto callCount = MockFailingSelectRows(/*failureCount*/ Max<int>());
    auto table = CreateTable(MakeTestRetryBackoffOptions(/*retryCount*/ 3));

    auto error = RunInFiber(BIND([&] {
        auto resultOrError = WaitFor(table->Select());
        EXPECT_THROW_WITH_SUBSTRING(
            resultOrError.ThrowOnError(),
            "Dynamic state request to //tmp/queue_agent/queues failed after 3 retries");
        // The error of the last attempt is attached to the resulting error.
        EXPECT_THROW_WITH_SUBSTRING(
            resultOrError.ThrowOnError(),
            "Transient failure");
    }));

    EXPECT_TRUE(error.IsOK())
        << ToString(error);
    // The request is performed once and then retried, so retryCount retries mean retryCount + 1 calls.
    EXPECT_EQ(4, *callCount);
}

TEST_F(TStateTableRetriesTest, ReconfigureRetryBackoff)
{
    auto callCount = MockFailingSelectRows(/*failureCount*/ 2);
    auto table = CreateTable();

    auto error = RunInFiber(BIND([&] {
        // Retries are disabled by default, so the very first failure is reported.
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(table->Select()).ThrowOnError(),
            "Transient failure");
        EXPECT_EQ(1, *callCount);

        table->ReconfigureRetryBackoff(MakeTestRetryBackoffOptions(/*retryCount*/ 3));

        EXPECT_NO_THROW(WaitFor(table->Select()).ThrowOnError());
        EXPECT_EQ(3, *callCount);
    }));

    EXPECT_TRUE(error.IsOK())
        << ToString(error);
}

TEST_F(TStateTableRetriesTest, InsertRetriesFailedCommit)
{
    auto commitCount = MockFailingCommit(/*failureCount*/ 2);
    auto table = CreateTable(MakeTestRetryBackoffOptions(/*retryCount*/ 3));
    auto rows = MakeQueueTableRows();

    auto error = RunInFiber(BIND([&] {
        EXPECT_NO_THROW(WaitFor(table->Insert(TRange(rows))).ThrowOnError());
    }));

    EXPECT_TRUE(error.IsOK())
        << ToString(error);
    EXPECT_EQ(3, *commitCount);
}

TEST_F(TStateTableRetriesTest, DeleteRetriesFailedCommit)
{
    auto commitCount = MockFailingCommit(/*failureCount*/ Max<int>());
    auto table = CreateTable(MakeTestRetryBackoffOptions(/*retryCount*/ 3));
    auto rows = MakeQueueTableRows();

    auto error = RunInFiber(BIND([&] {
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(table->Delete(TRange(rows))).ThrowOnError(),
            "Transient commit failure");
    }));

    EXPECT_TRUE(error.IsOK())
        << ToString(error);
    EXPECT_EQ(4, *commitCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueClient
