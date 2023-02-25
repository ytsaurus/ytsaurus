#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(OperationWatch)
{
    Y_UNIT_TEST(SimpleOperationWatch)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto operation = client->Sort(
            TSortOperationSpec()
                .SortBy({"foo"})
                .AddInput(workingDir + "/input")
                .Output(workingDir + "/output"),
            TOperationOptions().Wait(false));

        auto fut = operation->Watch();
        fut.Wait();
        fut.GetValue(); // no exception
        UNIT_ASSERT_VALUES_EQUAL(GetOperationState(client, operation->GetId()), "completed");

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT(operation->GetError().Empty());
    }

    Y_UNIT_TEST(FailedOperationWatch)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MaxFailedJobCount(1),
            new TAlwaysFailingMapper,
            TOperationOptions().Wait(false));

        auto fut = operation->Watch();
        fut.Wait();
        UNIT_ASSERT_EXCEPTION(fut.GetValue(), TOperationFailedError);
        UNIT_ASSERT_VALUES_EQUAL(GetOperationState(client, operation->GetId()), "failed");

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Failed);
        UNIT_ASSERT(operation->GetError().Defined());
    }

    void AbortedOperationWatchImpl(bool useOperationAbort)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MaxFailedJobCount(1),
            new TSleepingMapper(TDuration::Seconds(10)),
            TOperationOptions().Wait(false));

        if (useOperationAbort) {
            client->AbortOperation(operation->GetId());
        } else {
            operation->AbortOperation();
        }

        auto fut = operation->Watch();
        fut.Wait();
        UNIT_ASSERT_EXCEPTION(fut.GetValue(), TOperationFailedError);
        UNIT_ASSERT_VALUES_EQUAL(GetOperationState(client, operation->GetId()), "aborted");

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Aborted);
        UNIT_ASSERT(operation->GetError().Defined());
    }

    Y_UNIT_TEST(AbortedOperationWatch_ClientAbort)
    {
        AbortedOperationWatchImpl(false);
    }

    Y_UNIT_TEST(AbortedOperationWatch_OperationAbort)
    {
        AbortedOperationWatchImpl(true);
    }

    void CompletedOperationWatchImpl(bool useOperationComplete)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MaxFailedJobCount(1),
            new TSleepingMapper(TDuration::Seconds(3600)),
            TOperationOptions().Wait(false));

        while (GetOperationState(client, operation->GetId()) != "running") {
            Sleep(TDuration::MilliSeconds(100));
        }

        if (useOperationComplete) {
            client->CompleteOperation(operation->GetId());
        } else {
            operation->CompleteOperation();
        }

        auto fut = operation->Watch();
        fut.Wait(TDuration::Seconds(10));
        UNIT_ASSERT_NO_EXCEPTION(fut.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(GetOperationState(client, operation->GetId()), "completed");
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT(!operation->GetError().Defined());
    }

    Y_UNIT_TEST(CompletedOperationWatch_ClientComplete)
    {
        CompletedOperationWatchImpl(false);
    }

    Y_UNIT_TEST(CompletedOperationWatch_OperationComplete)
    {
        CompletedOperationWatchImpl(true);
    }

    void TestGetFailedJobInfoImpl(const IClientBasePtr& client, const TYPath& workingDir)
    {
        TConfig::Get()->UseAbortableResponse = true;
        auto outage = TAbortableHttpResponse::StartOutage("get_job_stderr", 2);

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MaxFailedJobCount(3),
            new TAlwaysFailingMapper(),
            TOperationOptions().Wait(false));
        operation->Watch().Wait();
        UNIT_ASSERT_EXCEPTION(operation->Watch().GetValue(), TOperationFailedError);

        auto failedJobInfoList = operation->GetFailedJobInfo(TGetFailedJobInfoOptions().MaxJobCount(10).StderrTailSize(1000));
        UNIT_ASSERT_VALUES_EQUAL(failedJobInfoList.size(), 3);
        for (const auto& jobInfo : failedJobInfoList) {
            UNIT_ASSERT(jobInfo.Error.ContainsText("User job failed"));
            UNIT_ASSERT_VALUES_EQUAL(jobInfo.Stderr, "This mapper always fails\n");
        }
    }

    Y_UNIT_TEST(GetFailedJobInfo_GlobalClient)
    {
        TTestFixture fixture;
        TestGetFailedJobInfoImpl(fixture.GetClient(), fixture.GetWorkingDir());
    }

    Y_UNIT_TEST(GetFailedJobInfo_Transaction)
    {
        TTestFixture fixture;
        TestGetFailedJobInfoImpl(fixture.GetClient()->StartTransaction(), fixture.GetWorkingDir());
    }

    Y_UNIT_TEST(GetBriefProgress)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto operation = client->Sort(
            TSortOperationSpec().SortBy({"foo"})
            .AddInput(workingDir + "/input")
            .Output(workingDir + "/output"),
            TOperationOptions().Wait(false));
        operation->Watch().Wait();

        WaitOperationHasBriefProgress(operation);

        // Request brief progress via poller.
        auto briefProgress = operation->GetBriefProgress();
        UNIT_ASSERT(briefProgress.Defined());
        UNIT_ASSERT(briefProgress->Total > 0);
    }

    Y_UNIT_TEST(TestHugeFailWithHugeStderr)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new THugeStderrMapper,
            TOperationOptions().Wait(false));

        //expect no exception
        operation->Watch().Wait();
    }

} // Y_UNIT_TEST_SUITE(OperationWatch)
