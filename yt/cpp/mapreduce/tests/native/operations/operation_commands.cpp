#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>

#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/sysstat.h>
#include <util/system/tempfile.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

class TIdMapperFailingFirstJob : public TIdMapper
{
public:
    void Start(TWriter*) override
    {
        if (FromString<int>(GetEnv("YT_JOB_INDEX")) == 1) {
            exit(1);
        }
    }
};
REGISTER_MAPPER(TIdMapperFailingFirstJob)

class TWriteFileThenSleepMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TWriteFileThenSleepMapper() = default;

    TWriteFileThenSleepMapper(TString fileName, TDuration sleepDuration)
        : FileName_(std::move(fileName))
        , SleepDuration_(sleepDuration)
    { }

    void Do(TReader*, TWriter* ) override
    {
        {
            TOFStream os(FileName_);
            os << "I'm here";
        }
        Sleep(SleepDuration_);
    }

    Y_SAVELOAD_JOB(FileName_, SleepDuration_);

private:
    TString FileName_;
    TDuration SleepDuration_;
};
REGISTER_MAPPER(TWriteFileThenSleepMapper)

////////////////////////////////////////////////////////////////////////////////

TEST(OperationCommands, GetBriefProgress)
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
        .Output(workingDir + "/output"));

    WaitOperationHasBriefProgress(operation);

    // Request brief progress directly
    auto briefProgress = operation->GetBriefProgress();
    EXPECT_TRUE(briefProgress.Defined());
    EXPECT_TRUE(briefProgress->Total > 0);
}

TEST(OperationCommands, JobCount)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/input").SortedBy({"foo"}));
        writer->AddRow(TNode()("foo", "bar"));
        writer->AddRow(TNode()("foo", "baz"));
        writer->AddRow(TNode()("foo", "qux"));
        writer->Finish();
    }

    auto getJobCount = [=] (const TOperationId& operationId) {
        WaitForPredicate([&] {
            return client->GetOperation(operationId).BriefProgress.Defined();
        });
        const auto& briefProgress = client->GetOperation(operationId).BriefProgress;
        EXPECT_TRUE(briefProgress);
        return briefProgress->Completed;
    };

    std::function<TOperationId(ui32,ui64)> runOperationFunctionList[] = {
        [=] (ui32 jobCount, ui64 dataSizePerJob) {
            auto mapSpec = TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output");
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
                .AddInput(workingDir + "/input")
                .Output(workingDir + "/output");
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
        EXPECT_EQ(static_cast<i64>(getJobCount(opId)), 1);

        opId = runOperationFunc(3, 0);
        EXPECT_EQ(static_cast<i64>(getJobCount(opId)), 3);

        opId = runOperationFunc(0, 1);
        EXPECT_EQ(static_cast<i64>(getJobCount(opId)), 3);

        opId = runOperationFunc(0, 100500);
        EXPECT_EQ(static_cast<i64>(getJobCount(opId)), 1);
    }
}

void TestGetOperation_Completed(bool useClientGetOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    auto beforeStart = TInstant::Now();
    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper);
    auto afterFinish = TInstant::Now();

    WaitOperationHasBriefProgress(op);

    TOperationAttributes attrs;
    if (useClientGetOperation) {
        attrs = client->GetOperation(op->GetId());
    } else {
        attrs = op->GetAttributes();
    }

    EXPECT_TRUE(attrs.Id);
    EXPECT_EQ(*attrs.Id, op->GetId());

    EXPECT_TRUE(attrs.Type);
    EXPECT_EQ(*attrs.Type, EOperationType::Map);

    EXPECT_TRUE(attrs.State);
    EXPECT_EQ(*attrs.State, "completed");

    EXPECT_TRUE(attrs.BriefState);
    EXPECT_EQ(*attrs.BriefState, EOperationBriefState::Completed);

    EXPECT_TRUE(attrs.AuthenticatedUser);
    EXPECT_EQ(*attrs.AuthenticatedUser, "root");

    EXPECT_TRUE(attrs.StartTime);
    EXPECT_TRUE(*attrs.StartTime > beforeStart);

    EXPECT_TRUE(attrs.FinishTime);
    EXPECT_TRUE(*attrs.FinishTime < afterFinish);

    EXPECT_TRUE(attrs.BriefProgress);
    EXPECT_TRUE(attrs.BriefProgress->Completed > 0);
    EXPECT_EQ(static_cast<i64>(attrs.BriefProgress->Failed), 0);

    auto stripEmptyAttributesInList = [] (auto list) {
        for (auto& node : list) {
            if (!node.HasAttributes()) {
                node.ClearAttributes();
            }
        }
        return list;
    };

    auto inputTables = TNode().Add(workingDir + "/input").AsList();
    EXPECT_TRUE(attrs.BriefSpec);
    EXPECT_TRUE(attrs.Spec);
    EXPECT_TRUE(attrs.FullSpec);
    EXPECT_EQ((*attrs.BriefSpec)["input_table_paths"].AsList(), inputTables);
    EXPECT_EQ((*attrs.Spec)["input_table_paths"].AsList(), inputTables);
    // NB(eshcherbin): Input table path from full spec comes with empty but existing attributes.
    EXPECT_EQ(stripEmptyAttributesInList((*attrs.FullSpec)["input_table_paths"].AsList()), inputTables);


    EXPECT_TRUE(attrs.Suspended);
    EXPECT_EQ(*attrs.Suspended, false);

    EXPECT_TRUE(attrs.Result);
    EXPECT_TRUE(!attrs.Result->Error);

    EXPECT_TRUE(attrs.Progress);
    auto row_count = client->Get(workingDir + "/input/@row_count").AsInt64();
    EXPECT_EQ(attrs.Progress->JobStatistics.GetStatistics("data/input/row_count").Sum(), row_count);

    EXPECT_TRUE(attrs.Events);
    for (const char* state : {"starting", "running", "completed"}) {
        EXPECT_TRUE(FindIfPtr(*attrs.Events, [=](const TOperationEvent& event) {
            return event.State == state;
        }));
    }
    EXPECT_TRUE(attrs.Events->front().Time > beforeStart);
    EXPECT_TRUE(attrs.Events->back().Time < afterFinish);
    for (size_t i = 1; i != attrs.Events->size(); ++i) {
        EXPECT_TRUE((*attrs.Events)[i].Time >= (*attrs.Events)[i - 1].Time);
    }

    // Can get operation with filter.

    auto options = TGetOperationOptions()
        .AttributeFilter(
            TOperationAttributeFilter()
            .Add(EOperationAttribute::Progress)
            .Add(EOperationAttribute::State));

    if (useClientGetOperation) {
        attrs = client->GetOperation(op->GetId(), options);
    } else {
        attrs = op->GetAttributes(options);
    }

    EXPECT_TRUE(!attrs.Id);
    EXPECT_TRUE(!attrs.Type);
    EXPECT_TRUE( attrs.State);
    EXPECT_TRUE(!attrs.AuthenticatedUser);
    EXPECT_TRUE(!attrs.StartTime);
    EXPECT_TRUE(!attrs.FinishTime);
    EXPECT_TRUE(!attrs.BriefProgress);
    EXPECT_TRUE(!attrs.BriefSpec);
    EXPECT_TRUE(!attrs.Spec);
    EXPECT_TRUE(!attrs.FullSpec);
    EXPECT_TRUE(!attrs.Suspended);
    EXPECT_TRUE(!attrs.Result);
    EXPECT_TRUE( attrs.Progress);
}

TEST(OperationCommands, GetOperation_Completed_ClientGetOperation)
{
    TestGetOperation_Completed(true);
}

TEST(OperationCommands, GetOperation_Completed_OperationGetAttributes)
{
    TestGetOperation_Completed(false);
}


void TestGetOperation_Failed(bool useClientGetOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MaxFailedJobCount(2),
        new TAlwaysFailingMapper,
        TOperationOptions()
            .Wait(false));

    op->Watch().Wait();

    WaitOperationHasBriefProgress(op);

    TOperationAttributes attrs;
    if (useClientGetOperation) {
        attrs = client->GetOperation(op->GetId());
    } else {
        attrs = op->GetAttributes();
    }

    EXPECT_TRUE(attrs.Type);
    EXPECT_EQ(*attrs.Type, EOperationType::Map);

    EXPECT_TRUE(attrs.BriefState);
    EXPECT_EQ(*attrs.BriefState, EOperationBriefState::Failed);

    EXPECT_TRUE(attrs.BriefProgress);
    EXPECT_EQ(static_cast<i64>(attrs.BriefProgress->Completed), 0);
    EXPECT_EQ(static_cast<i64>(attrs.BriefProgress->Failed), 2);

    EXPECT_TRUE(attrs.Result);
    EXPECT_TRUE(attrs.Result->Error);
    EXPECT_TRUE(attrs.Result->Error->ContainsText("Failed jobs limit exceeded"));
}

TEST(OperationCommands, GetOperation_Failed_ClientGetOperation)
{
    TestGetOperation_Failed(true);
}

TEST(OperationCommands, GetOperation_Failed_OperationGetAttributes)
{
    TestGetOperation_Failed(false);
}

TEST(OperationCommands, GetOperationAlert)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TSleepingMapper(TDuration::Seconds(100)),
        TOperationOptions()
            .Wait(false));

    WaitOperationIsRunning(op);

    op->CompleteOperation();

    EXPECT_NO_THROW(
        WaitForPredicate([&] {
            auto alerts = op->GetAttributes().Alerts;
            return alerts.Defined() && alerts->size() == 1 && alerts->contains("operation_completed_by_user_request");
        }));
}

TEST(OperationCommands, ListOperations)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TVector<IOperationPtr> operations;
    TVector<TInstant> beforeStartTimes;
    TVector<TInstant> afterFinishTimes;

    beforeStartTimes.push_back(TInstant::Now());
    auto mapOp = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MaxFailedJobCount(1),
        new TAlwaysFailingMapper,
        TOperationOptions()
            .Wait(false));
    EXPECT_THROW(mapOp->Watch().GetValueSync(), TOperationFailedError);
    operations.push_back(mapOp);
    afterFinishTimes.push_back(TInstant::Now());

    beforeStartTimes.push_back(TInstant::Now());
    operations.push_back(client->Sort(
        TSortOperationSpec()
            .AddInput(workingDir + "/input")
            .Output(workingDir + "/input")
            .SortBy({"foo"})));
    afterFinishTimes.push_back(TInstant::Now());

    beforeStartTimes.push_back(TInstant::Now());
    operations.push_back(client->Reduce(
        TReduceOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output-with-great-name")
            .ReduceBy({"foo"}),
        new TIdReducer));
    afterFinishTimes.push_back(TInstant::Now());

    for (const auto& operation : operations) {
        WaitOperationHasBriefProgress(operation);
    }

    EXPECT_NO_THROW(client->ListOperations());

    {
        auto result = client->ListOperations(
            TListOperationsOptions()
            .FromTime(beforeStartTimes.front())
            .ToTime(afterFinishTimes.back())
            .Limit(1)
            .IncludeCounters(true));

        EXPECT_EQ(std::ssize(result.Operations), 1);
        EXPECT_EQ(*result.Operations.front().Id, operations[2]->GetId());
    }
    {
        auto result = client->ListOperations(
            TListOperationsOptions()
            .FromTime(beforeStartTimes.front())
            .ToTime(afterFinishTimes.back())
            .Filter("output-with-great-name")
            .IncludeCounters(true));

        EXPECT_EQ(std::ssize(result.Operations), 1);
        EXPECT_EQ(*result.Operations.front().Id, operations[2]->GetId());
    }
    {
        auto result = client->ListOperations(
            TListOperationsOptions()
            .FromTime(beforeStartTimes.front())
            .ToTime(afterFinishTimes.back())
            .State("completed")
            .Type(EOperationType::Sort)
            .IncludeCounters(true));

        EXPECT_EQ(std::ssize(result.Operations), 1);
        EXPECT_EQ(*result.Operations.front().Id, operations[1]->GetId());
    }
    {
        auto result = client->ListOperations(
            TListOperationsOptions()
            .FromTime(beforeStartTimes.front())
            .ToTime(afterFinishTimes.back())
            .IncludeCounters(true));

        EXPECT_EQ(std::ssize(result.Operations), 3);
        const auto& attrs = result.Operations.front();

        EXPECT_TRUE(attrs.Id);
        // The order must be reversed: from newest to oldest.
        EXPECT_EQ(*attrs.Id, operations.back()->GetId());

        EXPECT_TRUE(attrs.BriefState);
        EXPECT_EQ(*attrs.BriefState, EOperationBriefState::Completed);

        EXPECT_TRUE(attrs.AuthenticatedUser);
        EXPECT_EQ(*attrs.AuthenticatedUser, "root");

        EXPECT_TRUE(result.PoolCounts);
        EXPECT_EQ(*result.PoolCounts, (THashMap<TString, i64>{{"root", 3}}));

        EXPECT_TRUE(result.UserCounts);
        EXPECT_EQ(*result.UserCounts, (THashMap<TString, i64>{{"root", 3}}));

        EXPECT_TRUE(result.StateCounts);
        EXPECT_EQ(*result.StateCounts, (THashMap<TString, i64>{{"completed", 2}, {"failed", 1}}));

        EXPECT_TRUE(result.TypeCounts);
        THashMap<EOperationType, i64> expectedTypeCounts = {
            {EOperationType::Map, 1},
            {EOperationType::Sort, 1},
            {EOperationType::Reduce, 1}};
        EXPECT_EQ(*result.TypeCounts, expectedTypeCounts);

        EXPECT_TRUE(result.WithFailedJobsCount);
        EXPECT_EQ(*result.WithFailedJobsCount, 1);
    }

    {
        auto result = client->ListOperations(
            TListOperationsOptions()
            .FromTime(beforeStartTimes.front())
            .ToTime(afterFinishTimes.back())
            .CursorTime(afterFinishTimes[1])
            .CursorDirection(ECursorDirection::Past));

        EXPECT_EQ(std::ssize(result.Operations), 2);

        EXPECT_TRUE(result.Operations[0].Id && result.Operations[1].Id);
        // The order must be reversed: from newest to oldest.
        EXPECT_EQ(*result.Operations[0].Id, operations[1]->GetId());
        EXPECT_EQ(*result.Operations[1].Id, operations[0]->GetId());
    }
}

TEST(OperationCommands, UpdateOperationParameters)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TSleepingMapper(TDuration::Seconds(100)),
        TOperationOptions()
            .Spec(TNode()("weight", 5.0))
            .Wait(false));

    Y_DEFER {
        op->AbortOperation();
    };

    static auto getState = [](const IOperationPtr& op) {
        auto attrs = op->GetAttributes(TGetOperationOptions().AttributeFilter(
            TOperationAttributeFilter().Add(EOperationAttribute::State)));
        return *attrs.BriefState;
    };

    while (getState(op) != EOperationBriefState::InProgress) {
        Sleep(TDuration::MilliSeconds(100));
    }

    client->UpdateOperationParameters(op->GetId(),
        TUpdateOperationParametersOptions()
        .SchedulingOptionsPerPoolTree(
            TSchedulingOptionsPerPoolTree()
            .Add("default", TSchedulingOptions()
                .Weight(10.0))));

    auto weightPath = "//sys/scheduler/orchid/scheduler/operations/" +
        GetGuidAsString(op->GetId()) +
        "/progress/scheduling_info_per_pool_tree/default/weight";

    WaitForPredicate([&] {
        try {
            return std::abs(client->Get(weightPath).AsDouble() - 10.0) < 1e-9;
        } catch (const std::exception& ex) {
            return false;
        }
    });
}

void TestSuspendResume(bool useOperationMethods)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "baz"));
        writer->Finish();
    }

    TTempFile tempFile(MakeTempName());
    Chmod(tempFile.Name().c_str(), 0777);

    auto operation = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TWriteFileThenSleepMapper(
            tempFile.Name(),
            TDuration::Seconds(300)),
        TOperationOptions().Wait(false));

    WaitForPredicate([&] {
        TIFStream is(tempFile.Name());
        return is.ReadAll().Size() > 0;
    });

    auto suspendOptions = TSuspendOperationOptions()
        .AbortRunningJobs(true);
    if (useOperationMethods) {
        operation->SuspendOperation(suspendOptions);
    } else {
        client->SuspendOperation(operation->GetId(), suspendOptions);
    }

    EXPECT_EQ(operation->GetAttributes().Suspended, true);

    WaitOperationHasBriefProgress(operation);
    WaitOperationPredicate(
        operation,
        [] (const TOperationAttributes& attrs) {
            return attrs.BriefProgress->Aborted >= 1;
        },
        "expected at least one aborted job");

    if (useOperationMethods) {
        operation->ResumeOperation();
    } else {
        client->ResumeOperation(operation->GetId());
    }

    EXPECT_EQ(operation->GetAttributes().Suspended, false);
}

TEST(OperationCommands, SuspendResume_OperationMethod)
{
    TestSuspendResume(true);
}

TEST(OperationCommands, SuspendResume_ClientMethod)
{
    TestSuspendResume(false);
}

TEST(OperationCommands, BatchOperationControl)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputPath = TRichYPath(workingDir + "/input");
    auto outputPath = TRichYPath(workingDir + "/output").Append(true);
    {
        auto writer = client->CreateTableWriter<TNode>(inputPath);
        writer->AddRow(TNode()("key", "key1")("value", "value1"));
        writer->Finish();
    }

    auto op1 = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputPath)
            .AddOutput<TNode>(outputPath),
        new TSleepingMapper(TDuration::Hours(1)),
        TOperationOptions().Wait(false));

    auto op2 = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputPath)
            .AddOutput<TNode>(outputPath),
        new TSleepingMapper(TDuration::Hours(1)),
        TOperationOptions().Wait(false));

    auto op3 = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputPath)
            .AddOutput<TNode>(outputPath),
        new TSleepingMapper(TDuration::Hours(1)),
        TOperationOptions()
        .Spec(TNode()("weight", 5.0))
        .Wait(false));

    WaitOperationIsRunning(op1);
    WaitOperationIsRunning(op2);
    WaitOperationIsRunning(op3);

    auto batchRequest = client->CreateBatchRequest();

    auto abortResult = batchRequest->AbortOperation(op1->GetId());
    auto completeResult = batchRequest->CompleteOperation(op2->GetId());
    auto updateOperationResult = batchRequest->UpdateOperationParameters(
        op3->GetId(),
        TUpdateOperationParametersOptions()
        .SchedulingOptionsPerPoolTree(
            TSchedulingOptionsPerPoolTree()
            .Add("default", TSchedulingOptions()
                .Weight(10.0))));

    EXPECT_EQ(op1->GetBriefState(), EOperationBriefState::InProgress);
    EXPECT_EQ(op2->GetBriefState(), EOperationBriefState::InProgress);
    EXPECT_EQ(op3->GetBriefState(), EOperationBriefState::InProgress);
    batchRequest->ExecuteBatch();

    // Check that there are no errors
    abortResult.GetValue();
    completeResult.GetValue();

    EXPECT_EQ(op1->GetBriefState(), EOperationBriefState::Aborted);
    EXPECT_EQ(op2->GetBriefState(), EOperationBriefState::Completed);

    WaitForPredicate([&] {
        auto weightPath = "//sys/scheduler/orchid/scheduler/operations/" +
            GetGuidAsString(op3->GetId()) +
            "/progress/scheduling_info_per_pool_tree/default/weight";
        try {
            return std::abs(client->Get(weightPath).AsDouble() - 10.0) < 1e-9;
        } catch (const std::exception& ex) {
            return false;
        }
    });

    op3->AbortOperation();
}

////////////////////////////////////////////////////////////////////////////////

TEST(JobCommands, GetJob)
{
    TTabletFixture tabletFixture;

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TStringBuf expectedStderr = "EXPECTED-STDERR";

    auto beforeStart = TInstant::Now();
    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .JobCount(1),
        new TMapperThatWritesStderr(expectedStderr));
    auto afterFinish = TInstant::Now();

    auto jobs = client->ListJobs(op->GetId()).Jobs;
    EXPECT_EQ(std::ssize(jobs), 1);
    EXPECT_TRUE(jobs.front().Id);
    auto jobId = *jobs.front().Id;

    for (const auto& job : {client->GetJob(op->GetId(), jobId), op->GetJob(jobId)}) {
        EXPECT_EQ(job.Id, jobId);
        EXPECT_EQ(job.State, EJobState::Completed);
        EXPECT_EQ(job.Type, EJobType::Map);

        EXPECT_TRUE(job.StartTime);
        EXPECT_TRUE(*job.StartTime > beforeStart);

        EXPECT_TRUE(job.FinishTime);
        EXPECT_TRUE(*job.FinishTime < afterFinish);
    }
}

TEST(JobCommands, ListJobs)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    auto beforeStart = TInstant::Now();
    auto op = client->MapReduce(
        TMapReduceOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .SortBy({"foo"})
            .ReduceBy({"foo"})
            .MapJobCount(2),
        new TIdMapperFailingFirstJob,
        /* reduceCombiner = */ nullptr,
        new TIdReducer);
    auto afterFinish = TInstant::Now();

    auto options = TListJobsOptions()
        .Type(EJobType::PartitionMap)
        .SortField(EJobSortField::State)
        .SortOrder(ESortOrder::SO_ASCENDING);

    for (const auto& result : {op->ListJobs(options), client->ListJobs(op->GetId(), options)}) {
        // There must be 3 partition_map jobs, the last of which is failed
        // (as EJobState::Failed > EJobState::Completed).

        for (size_t index = 0; index < result.Jobs.size(); ++index) {
            const auto& job = result.Jobs[index];

            EXPECT_TRUE(job.StartTime);
            EXPECT_TRUE(*job.StartTime > beforeStart);

            EXPECT_TRUE(job.FinishTime);
            EXPECT_TRUE(*job.FinishTime < afterFinish);

            EXPECT_TRUE(job.Type);
            EXPECT_EQ(*job.Type, EJobType::PartitionMap);

            EXPECT_TRUE(job.State);

            if (index == result.Jobs.size() - 1) {
                EXPECT_EQ(*job.State, EJobState::Failed);

                EXPECT_EQ(job.StderrSize.GetOrElse(0), 0);

                EXPECT_TRUE(job.Error);
                EXPECT_TRUE(job.Error->ContainsErrorCode(1205));
            } else {
                EXPECT_EQ(*job.State, EJobState::Completed);
            }

            EXPECT_TRUE(job.BriefStatistics);
        }
    }
}

TEST(JobCommands, GetJobInput)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    const TVector<TNode> expectedRows = {
        TNode()("a", 10)("b", 20),
        TNode()("a", 15)("b", 25),
    };

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        for (const auto& row : expectedRows) {
            writer->AddRow(row);
        }
        writer->Finish();
    }

    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .JobCount(1),
        new TSleepingMapper(TDuration::Seconds(100)),
        TOperationOptions()
            .Wait(false));

    Y_DEFER {
        op->AbortOperation();
    };

    auto isJobRunning = [&] () {
        auto jobs = op->ListJobs().Jobs;
        if (jobs.empty()) {
            return false;
        }
        const auto& job = jobs.front();
        TString path = ::TStringBuilder()
            << "//sys/cluster_nodes/" << *job.Address
            << "/orchid/exec_node/job_controller/active_jobs/" << *job.Id << "/job_phase";
        if (!client->Exists(path)) {
            return false;
        }
        return client->Get(path).AsString() == "running";
    };

    TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
    while (!isJobRunning() && TInstant::Now() < deadline) {
        Sleep(TDuration::MilliSeconds(100));
    }

    auto jobs = op->ListJobs().Jobs;
    EXPECT_EQ(std::ssize(jobs), 1);
    EXPECT_TRUE(jobs.front().Id.Defined());

    auto jobInputStream = client->GetJobInput(*jobs.front().Id);
    auto reader = CreateTableReader<TNode>(jobInputStream.Get());

    TVector<TNode> readRows;
    for (; reader->IsValid(); reader->Next()) {
        readRows.push_back(reader->MoveRow());
    }

    EXPECT_EQ(expectedRows, readRows);
}

TEST(JobCommands, GetJobStderr)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TStringBuf expectedStderr = "PYSHCH";
    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .JobCount(1),
        new TMapperThatWritesStderr(expectedStderr));

    auto jobs = op->ListJobs().Jobs;
    EXPECT_EQ(std::ssize(jobs), 1);
    EXPECT_TRUE(jobs.front().Id.Defined());

    auto jobStderrStream = client->GetJobStderr(op->GetId(), *jobs.front().Id);
    EXPECT_TRUE(jobStderrStream->ReadAll().Contains(expectedStderr));
}

TEST(JobCommands, GetJobFailContext)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    const TVector<TNode> expectedRows = {
        TNode()("a", 10)("b", 20),
        TNode()("a", 15)("b", 25),
    };

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        for (const auto& row : expectedRows) {
            writer->AddRow(row);
        }
        writer->Finish();
    }

    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .JobCount(1)
            .MaxFailedJobCount(1),
        new TAlwaysFailingMapper,
        TOperationOptions()
            .Wait(false));

    op->Watch().Wait();

    auto jobs = op->ListJobs().Jobs;
    EXPECT_EQ(std::ssize(jobs), 1);
    EXPECT_TRUE(jobs.front().Id.Defined());

    auto jobFailContextStream = client->GetJobFailContext(op->GetId(), *jobs.front().Id);
    auto reader = CreateTableReader<TNode>(jobFailContextStream.Get());

    TVector<TNode> readRows;
    for (; reader->IsValid(); reader->Next()) {
        readRows.push_back(reader->MoveRow());
    }

    EXPECT_EQ(expectedRows, readRows);
}
