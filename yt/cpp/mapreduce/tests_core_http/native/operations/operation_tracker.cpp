#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/library/operation_tracker/operation_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(OperationTracker)
{
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

    Y_UNIT_TEST(WaitAllCompleted_OkOperations)
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Completed);
    }

    Y_UNIT_TEST(WaitAllCompleted_ErrorOperations)
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

        UNIT_ASSERT_EXCEPTION(tracker.WaitAllCompleted(), TOperationFailedError);
    }

    Y_UNIT_TEST(WaitAllCompletedOrError_OkOperations)
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Completed);
    }

    Y_UNIT_TEST(WaitAllCompletedOrError_ErrorOperations)
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Failed);
    }

    Y_UNIT_TEST(WaitOneCompleted_OkOperation)
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
        UNIT_ASSERT(waited1);
        UNIT_ASSERT_VALUES_EQUAL(waited1->GetBriefState(), EOperationBriefState::Completed);

        auto waited2 = tracker.WaitOneCompleted();
        UNIT_ASSERT(waited2);
        UNIT_ASSERT_VALUES_EQUAL(waited2->GetBriefState(), EOperationBriefState::Completed);

        auto waited3 = tracker.WaitOneCompleted();
        UNIT_ASSERT(!waited3);
        UNIT_ASSERT_VALUES_EQUAL(TSet<IOperation*>({op1.Get(), op2.Get()}), TSet<IOperation*>({waited1.Get(), waited2.Get()}));
    }

    Y_UNIT_TEST(WaitOneCompleted_ErrorOperation)
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

        UNIT_ASSERT_EXCEPTION(waitByOne(), TOperationFailedError);
    }

    Y_UNIT_TEST(WaitOneCompletedOrError_ErrorOperation)
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
        UNIT_ASSERT(waited1);

        auto waited2 = tracker.WaitOneCompletedOrError();
        UNIT_ASSERT(waited2);

        auto waited3 = tracker.WaitOneCompletedOrError();
        UNIT_ASSERT(!waited3);

        UNIT_ASSERT_VALUES_EQUAL(TSet<IOperation*>({op1.Get(), op2.Get()}), TSet<IOperation*>({waited1.Get(), waited2.Get()}));
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Failed);
    }

    Y_UNIT_TEST(ConnectionErrorWhenOperationIsTracked)
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
        UNIT_ASSERT_VALUES_EQUAL(res, true);
        UNIT_ASSERT_EXCEPTION(fut.GetValue(), yexception);
        UNIT_ASSERT(ytPollerTopLoopCounter.GetTotal() > 0);
        outage.Stop();

        tx->Abort(); // We make sure that operation is stopped
    }

} // Y_UNIT_TEST_SUITE(OperationTracker)
