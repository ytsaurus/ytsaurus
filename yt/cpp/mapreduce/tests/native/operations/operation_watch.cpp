#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

namespace {

TEST(OperationWatch, SimpleOperationWatch)
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
    EXPECT_EQ(GetOperationState(client, operation->GetId()), "completed");

    EmulateOperationArchivation(client, operation->GetId());
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_TRUE(operation->GetError().Empty());
}

TEST(OperationWatch, FailedOperationWatch)
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
    EXPECT_THROW(fut.GetValue(), TOperationFailedError);
    EXPECT_EQ(GetOperationState(client, operation->GetId()), "failed");

    EmulateOperationArchivation(client, operation->GetId());
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Failed);
    EXPECT_TRUE(operation->GetError().Defined());
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
    EXPECT_THROW(fut.GetValue(), TOperationFailedError);
    EXPECT_EQ(GetOperationState(client, operation->GetId()), "aborted");

    EmulateOperationArchivation(client, operation->GetId());
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Aborted);
    EXPECT_TRUE(operation->GetError().Defined());
}

TEST(OperationWatch, AbortedOperationWatch_ClientAbort)
{
    AbortedOperationWatchImpl(false);
}

TEST(OperationWatch, AbortedOperationWatch_OperationAbort)
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
    EXPECT_NO_THROW(fut.GetValue());
    EXPECT_EQ(GetOperationState(client, operation->GetId()), "completed");
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_TRUE(!operation->GetError().Defined());
}

TEST(OperationWatch, CompletedOperationWatch_ClientComplete)
{
    CompletedOperationWatchImpl(false);
}

TEST(OperationWatch, CompletedOperationWatch_OperationComplete)
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
    EXPECT_THROW(operation->Watch().GetValue(), TOperationFailedError);

    auto failedJobInfoList = operation->GetFailedJobInfo(TGetFailedJobInfoOptions().MaxJobCount(10).StderrTailSize(1000));
    EXPECT_EQ(std::ssize(failedJobInfoList), 3);
    for (const auto& jobInfo : failedJobInfoList) {
        EXPECT_TRUE(jobInfo.Error.ContainsText("User job failed"));
        EXPECT_EQ(jobInfo.Stderr, "This mapper always fails\n");
    }
}

TEST(OperationWatch, GetFailedJobInfo_GlobalClient)
{
    TTestFixture fixture;
    TestGetFailedJobInfoImpl(fixture.GetClient(), fixture.GetWorkingDir());
}

TEST(OperationWatch, GetFailedJobInfo_Transaction)
{
    TTestFixture fixture;
    TestGetFailedJobInfoImpl(fixture.GetClient()->StartTransaction(), fixture.GetWorkingDir());
}

TEST(OperationWatch, GetBriefProgress)
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
    EXPECT_TRUE(briefProgress.Defined());
    EXPECT_TRUE(briefProgress->Total > 0);
}

TEST(OperationWatch, TestHugeFailWithHugeStderr)
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

} // namespace
