#include "lib.h"

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/helpers.h>

#include <library/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/folder/path.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

static TString GetOperationState(const IClientPtr& client, const TOperationId& operationId)
{
    return client->Get("//sys/operations/" + GetGuidAsString(operationId) + "/@state").AsString();
}

static void EmulateOperationArchivation(IClientPtr& client, const TOperationId& operationId)
{
    client->Remove("//sys/operations/" + GetGuidAsString(operationId), TRemoveOptions().Recursive(true));
}

////////////////////////////////////////////////////////////////////////////////

class TIdMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};
REGISTER_MAPPER(TIdMapper);

////////////////////////////////////////////////////////////////////////////////

class TIdReducer : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};
REGISTER_REDUCER(TIdReducer);

////////////////////////////////////////////////////////////////////

class TAlwaysFailingMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader*, TWriter*)
    {
        Cerr << "This mapper always fails" << Endl;
        ::exit(1);
    }
};
REGISTER_MAPPER(TAlwaysFailingMapper);

////////////////////////////////////////////////////////////////////

class TMapperThatWritesStderr : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter*) {
        for (; reader->IsValid(); reader->Next()) {
        }
        Cerr << "PYSHCH" << Endl;
    }
};
REGISTER_MAPPER(TMapperThatWritesStderr);

////////////////////////////////////////////////////////////////////

class TMapperThatWritesToIncorrectTable : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader*, TWriter* writer) {
        try {
            writer->AddRow(TNode(), 100500);
        } catch (...) {
        }
    }
};
REGISTER_MAPPER(TMapperThatWritesToIncorrectTable);

////////////////////////////////////////////////////////////////////////////////

class TMapperThatChecksFile : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TMapperThatChecksFile() = default;
    TMapperThatChecksFile(const TString& file)
        : File_(file)
    { }

    virtual void Do(TReader*, TWriter*) override {
        if (!TFsPath(File_).Exists()) {
            Cerr << "File `" << File_ << "' does not exist." << Endl;
            exit(1);
        }
    }

    Y_SAVELOAD_JOB(File_);

private:
    TString File_;
};
REGISTER_MAPPER(TMapperThatChecksFile);

////////////////////////////////////////////////////////////////////

class TSleepingMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TSleepingMapper() = default;

    TSleepingMapper(TDuration sleepDuration)
        : SleepDuration_(sleepDuration)
    { }

    virtual void Do(TReader*, TWriter* ) override
    {
        Sleep(SleepDuration_);
    }

    Y_SAVELOAD_JOB(SleepDuration_);

private:
    TDuration SleepDuration_;
};
REGISTER_MAPPER(TSleepingMapper);

////////////////////////////////////////////////////////////////////

SIMPLE_UNIT_TEST_SUITE(Operations)
{
    SIMPLE_UNIT_TEST(IncorrectTableId)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .MaxFailedJobCount(1),
            new TMapperThatWritesToIncorrectTable);
    }

    SIMPLE_UNIT_TEST(MaxFailedJobCount)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        for (const auto maxFail : {1, 7}) {
            TOperationId operationId;
            try {
                client->Map(
                    TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output")
                    .MaxFailedJobCount(maxFail),
                    new TAlwaysFailingMapper);
                UNIT_FAIL("operation expected to fail");
            } catch (const TOperationFailedError& e) {
                operationId = e.GetOperationId();
            }

            {
                auto failedJobs = client->Get(TStringBuilder() << "//sys/operations/" << operationId << "/@brief_progress/jobs/failed");
                UNIT_ASSERT_VALUES_EQUAL(failedJobs.AsInt64(), maxFail);
            }
        }
    }

    SIMPLE_UNIT_TEST(StderrTablePath)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .StderrTablePath("//testing/stderr"),
            new TMapperThatWritesStderr);

        auto reader = client->CreateTableReader<TNode>("//testing/stderr");
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow()["data"].AsString(), "PYSHCH\n");
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(CreateDebugOutputTables)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        // stderr table does not exist => should fail
        try {
            client->Map(
                TMapOperationSpec().CreateDebugOutputTables(false)
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output")
                .StderrTablePath("//testing/stderr"),
                new TMapperThatWritesStderr);
            UNIT_FAIL("operation expected to fail");
        } catch (const TOperationFailedError& e) {
        }

        client->Create("//testing/stderr", NT_TABLE);

        // stderr table exists => should pass
        client->Map(
            TMapOperationSpec().CreateDebugOutputTables(false)
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .StderrTablePath("//testing/stderr"),
            new TMapperThatWritesStderr);
    }

    SIMPLE_UNIT_TEST(CreateOutputTables)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        // Output table does not exist => operation should fail.
        try {
            client->Map(
                TMapOperationSpec().CreateOutputTables(false)
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output")
                .StderrTablePath("//testing/stderr"),
                new TMapperThatWritesStderr);
            UNIT_FAIL("operation expected to fail");
        } catch (const TOperationFailedError& e) {
        }

        client->Create("//testing/output", NT_TABLE);

        // Output table exists => should complete ok.
        client->Map(
            TMapOperationSpec().CreateDebugOutputTables(false)
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .StderrTablePath("//testing/stderr"),
            new TMapperThatWritesStderr);
    }

    SIMPLE_UNIT_TEST(JobCount)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath("//testing/input").SortedBy({"foo"}));
            writer->AddRow(TNode()("foo", "bar"));
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "qux"));
            writer->Finish();
        }

        auto getJobCount = [=] (const TOperationId& operationId) {
            TYPath operationPath = "//sys/operations/" + GetGuidAsString(operationId) + "/@brief_progress/jobs/completed";
            return client->Get(operationPath).AsInt64();
        };

        std::function<TOperationId(ui32,ui64)> runOperationFunctionList[] = {
            [=] (ui32 jobCount, ui64 dataSizePerJob) {
                auto mapSpec = TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output");
                if (jobCount) {
                    mapSpec.JobCount(jobCount);
                }
                if (dataSizePerJob) {
                    mapSpec.DataSizePerJob(dataSizePerJob);
                }
                return client->Map(mapSpec, new TIdMapper)->GetId();
            },
            [=] (ui32 jobCount, ui64 dataSizePerJob) {
                auto mergeSpec = TMergeOperationSpec()
                    .ForceTransform(true)
                    .AddInput("//testing/input")
                    .Output("//testing/output");
                if (jobCount) {
                    mergeSpec.JobCount(jobCount);
                }
                if (dataSizePerJob) {
                    mergeSpec.DataSizePerJob(dataSizePerJob);
                }
                return client->Merge(mergeSpec)->GetId();
            },
        };

        for (const auto& runOperationFunc : runOperationFunctionList) {
            auto opId = runOperationFunc(1, 0);
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 1);

            opId = runOperationFunc(3, 0);
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 3);

            opId = runOperationFunc(0, 1);
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 3);

            opId = runOperationFunc(0, 100500);
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 1);
        }
    }

    SIMPLE_UNIT_TEST(TestFetchTable)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        // Expect operation to complete successfully
        client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .MapperSpec(TUserJobSpec().AddFile(TRichYPath("//testing/input").Format("yson"))),
            new TMapperThatChecksFile("input"));
    }

    SIMPLE_UNIT_TEST(TestFetchTableRange)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        // Expect operation to complete successfully
        client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .MapperSpec(TUserJobSpec().AddFile(TRichYPath("//testing/input[#0]").Format("yson"))),
            new TMapperThatChecksFile("input"));
    }

    SIMPLE_UNIT_TEST(TestGetOperationStatus_Completed)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto operation = client->Sort(
            TSortOperationSpec().SortBy({"foo"})
            .AddInput("//testing/input")
            .Output("//testing/output"),
            TOperationOptions().Wait(false));

        while (operation->GetStatus() == OS_IN_PROGRESS) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatus(), OS_COMPLETED);
        UNIT_ASSERT(operation->GetError().Empty());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatus(), OS_COMPLETED);
        UNIT_ASSERT(operation->GetError().Empty());
    }

    SIMPLE_UNIT_TEST(TestGetOperationStatus_Failed)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .MaxFailedJobCount(1),
            new TAlwaysFailingMapper,
            TOperationOptions().Wait(false));

        while (operation->GetStatus() == OS_IN_PROGRESS) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatus(), OS_FAILED);
        UNIT_ASSERT(operation->GetError().Defined());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatus(), OS_FAILED);
        UNIT_ASSERT(operation->GetError().Defined());
    }

    SIMPLE_UNIT_TEST(TestGetOperationStatistics)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto operation = client->Sort(
            TSortOperationSpec().SortBy({"foo"})
            .AddInput("//testing/input")
            .Output("//testing/output"));
        auto jobStatistics = operation->GetJobStatistics();
        UNIT_ASSERT(jobStatistics.GetStatistics("time/total").Max().Defined());
    }

    SIMPLE_UNIT_TEST(GetBriefProgress)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto operation = client->Sort(
            TSortOperationSpec().SortBy({"foo"})
            .AddInput("//testing/input")
            .Output("//testing/output"));
        // Request brief progress directly
        auto briefProgress = operation->GetBriefProgress();
        UNIT_ASSERT(briefProgress.Defined());
        UNIT_ASSERT(briefProgress->Total > 0);
    }
}

SIMPLE_UNIT_TEST_SUITE(OperationWatch)
{
    SIMPLE_UNIT_TEST(SimpleOperationWatch)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto operation = client->Sort(
            TSortOperationSpec().SortBy({"foo"})
            .AddInput("//testing/input")
            .Output("//testing/output"),
            TOperationOptions().Wait(false));

        auto fut = operation->Watch();
        fut.Wait();
        fut.GetValue(); // no exception
        UNIT_ASSERT_VALUES_EQUAL(GetOperationState(client, operation->GetId()), "completed");

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatus(), OS_COMPLETED);
        UNIT_ASSERT(operation->GetError().Empty());
    }

    SIMPLE_UNIT_TEST(FailedOperationWatch)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .MaxFailedJobCount(1),
            new TAlwaysFailingMapper,
            TOperationOptions().Wait(false));

        auto fut = operation->Watch();
        fut.Wait();
        UNIT_ASSERT_EXCEPTION(fut.GetValue(), TOperationFailedError);
        UNIT_ASSERT_VALUES_EQUAL(GetOperationState(client, operation->GetId()), "failed");

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatus(), OS_FAILED);
        UNIT_ASSERT(operation->GetError().Defined());
    }

    void AbortedOperationWatchImpl(bool useOperationAbort)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatus(), OS_ABORTED);
        UNIT_ASSERT(operation->GetError().Defined());
    }

    SIMPLE_UNIT_TEST(AbortedOperationWatch_ClientAbort)
    {
        AbortedOperationWatchImpl(false);
    }

    SIMPLE_UNIT_TEST(AbortedOperationWatch_OperationAbort)
    {
        AbortedOperationWatchImpl(true);
    }

    void TestGetFailedJobInfoImpl(IClientBasePtr client)
    {
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
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

    SIMPLE_UNIT_TEST(GetFailedJobInfo_GlobalClient)
    {
        TestGetFailedJobInfoImpl(CreateTestClient());
    }

    SIMPLE_UNIT_TEST(GetFailedJobInfo_Transaction)
    {
        TestGetFailedJobInfoImpl(CreateTestClient()->StartTransaction());
    }

    SIMPLE_UNIT_TEST(GetBriefProgress)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto operation = client->Sort(
            TSortOperationSpec().SortBy({"foo"})
            .AddInput("//testing/input")
            .Output("//testing/output"),
            TOperationOptions().Wait(false));
        operation->Watch().Wait();
        // Request brief progress via poller
        auto briefProgress = operation->GetBriefProgress();
        UNIT_ASSERT(briefProgress.Defined());
        UNIT_ASSERT(briefProgress->Total > 0);
    }
}

////////////////////////////////////////////////////////////////////
