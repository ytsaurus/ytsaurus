#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/library/operation_tracker/operation_tracker.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/set.h>

using namespace NYT;
using namespace NYT::NTesting;

namespace {

IOperationPtr AsyncSortByFoo(IClientPtr client, const TString& input, const TString& output)
{
    return client->Sort(
        TSortOperationSpec().SortBy({"foo"})
        .AddInput(input)
        .Output(output),
        TOperationOptions().Wait(false));
}

IOperationPtr AsyncAlwaysFailingMapper(IClientPtr client, const TString& input, const TString& output)
{
    return client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(input)
            .AddOutput<TNode>(output)
            .MaxFailedJobCount(1),
        new TAlwaysFailingMapper,
        TOperationOptions().Wait(false));
}

TEST(OperationTracker, WaitAllCompleted_OkOperations)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TOperationTracker tracker;

    auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
    tracker.AddOperation(op1);
    auto op2 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output2");
    tracker.AddOperation(op2);

    tracker.WaitAllCompleted();
    EXPECT_EQ(op1->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_EQ(op2->GetBriefState(), EOperationBriefState::Completed);
}

TEST(OperationTracker, WaitAllCompleted_ErrorOperations)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TOperationTracker tracker;

    auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
    tracker.AddOperation(op1);
    auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
    tracker.AddOperation(op2);

    EXPECT_THROW(tracker.WaitAllCompleted(), TOperationFailedError);
}

TEST(OperationTracker, WaitAllCompletedOrError_OkOperations)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TOperationTracker tracker;

    auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
    tracker.AddOperation(op1);
    auto op2 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output2");
    tracker.AddOperation(op2);

    tracker.WaitAllCompletedOrError();
    EXPECT_EQ(op1->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_EQ(op2->GetBriefState(), EOperationBriefState::Completed);
}

TEST(OperationTracker, WaitAllCompletedOrError_ErrorOperations)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TOperationTracker tracker;

    auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
    tracker.AddOperation(op1);
    auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
    tracker.AddOperation(op2);

    tracker.WaitAllCompletedOrError();
    EXPECT_EQ(op1->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_EQ(op2->GetBriefState(), EOperationBriefState::Failed);
}

TEST(OperationTracker, WaitOneCompleted_OkOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TOperationTracker tracker;

    auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
    tracker.AddOperation(op1);
    auto op2 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output2");
    tracker.AddOperation(op2);

    auto waited1 = tracker.WaitOneCompleted();
    EXPECT_TRUE(waited1);
    EXPECT_EQ(waited1->GetBriefState(), EOperationBriefState::Completed);

    auto waited2 = tracker.WaitOneCompleted();
    EXPECT_TRUE(waited2);
    EXPECT_EQ(waited2->GetBriefState(), EOperationBriefState::Completed);

    auto waited3 = tracker.WaitOneCompleted();
    EXPECT_TRUE(!waited3);
    EXPECT_EQ(TSet<IOperation*>({op1.Get(), op2.Get()}), TSet<IOperation*>({waited1.Get(), waited2.Get()}));
}

TEST(OperationTracker, WaitOneCompleted_ErrorOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TOperationTracker tracker;

    auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
    tracker.AddOperation(op1);
    auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
    tracker.AddOperation(op2);

    auto waitByOne = [&] {
        auto waited1 = tracker.WaitOneCompleted();
        auto waited2 = tracker.WaitOneCompleted();
    };

    EXPECT_THROW(waitByOne(), TOperationFailedError);
}

TEST(OperationTracker, WaitOneCompletedOrError_ErrorOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TOperationTracker tracker;

    auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
    tracker.AddOperation(op1);
    auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
    tracker.AddOperation(op2);

    auto waited1 = tracker.WaitOneCompletedOrError();
    EXPECT_TRUE(waited1);

    auto waited2 = tracker.WaitOneCompletedOrError();
    EXPECT_TRUE(waited2);

    auto waited3 = tracker.WaitOneCompletedOrError();
    EXPECT_TRUE(!waited3);

    EXPECT_EQ(TSet<IOperation*>({op1.Get(), op2.Get()}), TSet<IOperation*>({waited1.Get(), waited2.Get()}));
    EXPECT_EQ(op1->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_EQ(op2->GetBriefState(), EOperationBriefState::Failed);
}

TEST(OperationTracker, ConnectionErrorWhenOperationIsTracked)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->EnableDebugMetrics = true;
    TConfig::Get()->RetryCount = 1;
    TConfig::Get()->ReadRetryCount = 1;
    TConfig::Get()->StartOperationRetryCount = 1;
    TConfig::Get()->WaitLockPollInterval = TDuration::MilliSeconds(0);


    CreateTableWithFooColumn(client, workingDir + "/input");
    auto tx = client->StartTransaction();

    auto op = tx->Map(
        TMapOperationSpec()
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper(),
        TOperationOptions().Wait(false));

    auto outage = TAbortableHttpResponse::StartOutage("");
    TDebugMetricDiff ytPollerTopLoopCounter("yt_poller_top_loop_repeat_count");

    auto fut = op->Watch();
    auto res = fut.Wait(TDuration::MilliSeconds(500));
    EXPECT_EQ(res, true);
    EXPECT_THROW(fut.GetValue(), yexception);
    EXPECT_TRUE(ytPollerTopLoopCounter.GetTotal() > 0);
    outage.Stop();

    tx->Abort(); // We make sure that operation is stopped
}

} // namespace
