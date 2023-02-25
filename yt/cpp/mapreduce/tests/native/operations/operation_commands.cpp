#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

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
REGISTER_MAPPER(TIdMapperFailingFirstJob);

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
REGISTER_MAPPER(TWriteFileThenSleepMapper);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(OperationCommands)
{

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
            .Output(workingDir + "/output"));

        WaitOperationHasBriefProgress(operation);

        // Request brief progress directly
        auto briefProgress = operation->GetBriefProgress();
        UNIT_ASSERT(briefProgress.Defined());
        UNIT_ASSERT(briefProgress->Total > 0);
    }

    Y_UNIT_TEST(JobCount)
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
            UNIT_ASSERT(briefProgress);
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
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 1);

            opId = runOperationFunc(3, 0);
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 3);

            opId = runOperationFunc(0, 1);
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 3);

            opId = runOperationFunc(0, 100500);
            UNIT_ASSERT_VALUES_EQUAL(getJobCount(opId), 1);
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

        UNIT_ASSERT(attrs.Id);
        UNIT_ASSERT_EQUAL(*attrs.Id, op->GetId());

        UNIT_ASSERT(attrs.Type);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.Type, EOperationType::Map);

        UNIT_ASSERT(attrs.State);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.State, "completed");

        UNIT_ASSERT(attrs.BriefState);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.BriefState, EOperationBriefState::Completed);

        UNIT_ASSERT(attrs.AuthenticatedUser);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.AuthenticatedUser, "root");

        UNIT_ASSERT(attrs.StartTime);
        UNIT_ASSERT(*attrs.StartTime > beforeStart);

        UNIT_ASSERT(attrs.FinishTime);
        UNIT_ASSERT(*attrs.FinishTime < afterFinish);

        UNIT_ASSERT(attrs.BriefProgress);
        UNIT_ASSERT(attrs.BriefProgress->Completed > 0);
        UNIT_ASSERT_VALUES_EQUAL(attrs.BriefProgress->Failed, 0);

        auto stripEmptyAttributesInList = [] (auto list) {
            for (auto& node : list) {
                if (!node.HasAttributes()) {
                    node.ClearAttributes();
                }
            }
            return list;
        };

        auto inputTables = TNode().Add(workingDir + "/input").AsList();
        UNIT_ASSERT(attrs.BriefSpec);
        UNIT_ASSERT(attrs.Spec);
        UNIT_ASSERT(attrs.FullSpec);
        UNIT_ASSERT_VALUES_EQUAL((*attrs.BriefSpec)["input_table_paths"].AsList(), inputTables);
        UNIT_ASSERT_VALUES_EQUAL((*attrs.Spec)["input_table_paths"].AsList(), inputTables);
        // NB(eshcherbin): Input table path from full spec comes with empty but existing attributes.
        UNIT_ASSERT_VALUES_EQUAL(stripEmptyAttributesInList((*attrs.FullSpec)["input_table_paths"].AsList()), inputTables);


        UNIT_ASSERT(attrs.Suspended);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.Suspended, false);

        UNIT_ASSERT(attrs.Result);
        UNIT_ASSERT(!attrs.Result->Error);

        UNIT_ASSERT(attrs.Progress);
        auto row_count = client->Get(workingDir + "/input/@row_count").AsInt64();
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobStatistics.GetStatistics("data/input/row_count").Sum(), row_count);

        UNIT_ASSERT(attrs.Events);
        for (const char* state : {"starting", "running", "completed"}) {
            UNIT_ASSERT(FindIfPtr(*attrs.Events, [=](const TOperationEvent& event) {
                return event.State == state;
            }));
        }
        UNIT_ASSERT(attrs.Events->front().Time > beforeStart);
        UNIT_ASSERT(attrs.Events->back().Time < afterFinish);
        for (size_t i = 1; i != attrs.Events->size(); ++i) {
            UNIT_ASSERT((*attrs.Events)[i].Time >= (*attrs.Events)[i - 1].Time);
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

        UNIT_ASSERT(!attrs.Id);
        UNIT_ASSERT(!attrs.Type);
        UNIT_ASSERT( attrs.State);
        UNIT_ASSERT(!attrs.AuthenticatedUser);
        UNIT_ASSERT(!attrs.StartTime);
        UNIT_ASSERT(!attrs.FinishTime);
        UNIT_ASSERT(!attrs.BriefProgress);
        UNIT_ASSERT(!attrs.BriefSpec);
        UNIT_ASSERT(!attrs.Spec);
        UNIT_ASSERT(!attrs.FullSpec);
        UNIT_ASSERT(!attrs.Suspended);
        UNIT_ASSERT(!attrs.Result);
        UNIT_ASSERT( attrs.Progress);
    }

    Y_UNIT_TEST(GetOperation_Completed_ClientGetOperation)
    {
        TestGetOperation_Completed(true);
    }

    Y_UNIT_TEST(GetOperation_Completed_OperationGetAttributes)
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

        UNIT_ASSERT(attrs.Type);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.Type, EOperationType::Map);

        UNIT_ASSERT(attrs.BriefState);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.BriefState, EOperationBriefState::Failed);

        UNIT_ASSERT(attrs.BriefProgress);
        UNIT_ASSERT_VALUES_EQUAL(attrs.BriefProgress->Completed, 0);
        UNIT_ASSERT_VALUES_EQUAL(attrs.BriefProgress->Failed, 2);

        UNIT_ASSERT(attrs.Result);
        UNIT_ASSERT(attrs.Result->Error);
        UNIT_ASSERT(attrs.Result->Error->ContainsText("Failed jobs limit exceeded"));
    }

    Y_UNIT_TEST(GetOperation_Failed_ClientGetOperation)
    {
        TestGetOperation_Failed(true);
    }

    Y_UNIT_TEST(GetOperation_Failed_OperationGetAttributes)
    {
        TestGetOperation_Failed(false);
    }

    Y_UNIT_TEST(GetOperationAlert)
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

        UNIT_ASSERT_NO_EXCEPTION(
            WaitForPredicate([&] {
                auto alerts = op->GetAttributes().Alerts;
                return alerts.Defined() && alerts->size() == 1 && alerts->contains("operation_completed_by_user_request");
            }));
    }

    Y_UNIT_TEST(ListOperations)
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
        UNIT_ASSERT_EXCEPTION(mapOp->Watch().GetValueSync(), TOperationFailedError);
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

        {
            auto result = client->ListOperations(
                TListOperationsOptions()
                .FromTime(beforeStartTimes.front())
                .ToTime(afterFinishTimes.back())
                .Limit(1)
                .IncludeCounters(true));

            UNIT_ASSERT_VALUES_EQUAL(result.Operations.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(*result.Operations.front().Id, operations[2]->GetId());
        }
        {
            auto result = client->ListOperations(
                TListOperationsOptions()
                .FromTime(beforeStartTimes.front())
                .ToTime(afterFinishTimes.back())
                .Filter("output-with-great-name")
                .IncludeCounters(true));

            UNIT_ASSERT_VALUES_EQUAL(result.Operations.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(*result.Operations.front().Id, operations[2]->GetId());
        }
        {
            auto result = client->ListOperations(
                TListOperationsOptions()
                .FromTime(beforeStartTimes.front())
                .ToTime(afterFinishTimes.back())
                .State("completed")
                .Type(EOperationType::Sort)
                .IncludeCounters(true));

            UNIT_ASSERT_VALUES_EQUAL(result.Operations.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(*result.Operations.front().Id, operations[1]->GetId());
        }
        {
            auto result = client->ListOperations(
                TListOperationsOptions()
                .FromTime(beforeStartTimes.front())
                .ToTime(afterFinishTimes.back())
                .IncludeCounters(true));

            UNIT_ASSERT_VALUES_EQUAL(result.Operations.size(), 3);
            const auto& attrs = result.Operations.front();

            UNIT_ASSERT(attrs.Id);
            // The order must be reversed: from newest to oldest.
            UNIT_ASSERT_VALUES_EQUAL(*attrs.Id, operations.back()->GetId());

            UNIT_ASSERT(attrs.BriefState);
            UNIT_ASSERT_VALUES_EQUAL(*attrs.BriefState, EOperationBriefState::Completed);

            UNIT_ASSERT(attrs.AuthenticatedUser);
            UNIT_ASSERT_VALUES_EQUAL(*attrs.AuthenticatedUser, "root");

            UNIT_ASSERT(result.PoolCounts);
            UNIT_ASSERT_VALUES_EQUAL(*result.PoolCounts, (THashMap<TString, i64>{{"root", 3}}));

            UNIT_ASSERT(result.UserCounts);
            UNIT_ASSERT_VALUES_EQUAL(*result.UserCounts, (THashMap<TString, i64>{{"root", 3}}));

            UNIT_ASSERT(result.StateCounts);
            UNIT_ASSERT_VALUES_EQUAL(*result.StateCounts, (THashMap<TString, i64>{{"completed", 2}, {"failed", 1}}));

            UNIT_ASSERT(result.TypeCounts);
            THashMap<EOperationType, i64> expectedTypeCounts = {
                {EOperationType::Map, 1},
                {EOperationType::Sort, 1},
                {EOperationType::Reduce, 1}};
            UNIT_ASSERT_VALUES_EQUAL(*result.TypeCounts, expectedTypeCounts);

            UNIT_ASSERT(result.WithFailedJobsCount);
            UNIT_ASSERT_VALUES_EQUAL(*result.WithFailedJobsCount, 1);
        }

        {
            auto result = client->ListOperations(
                TListOperationsOptions()
                .FromTime(beforeStartTimes.front())
                .ToTime(afterFinishTimes.back())
                .CursorTime(afterFinishTimes[1])
                .CursorDirection(ECursorDirection::Past));

            UNIT_ASSERT_VALUES_EQUAL(result.Operations.size(), 2);

            UNIT_ASSERT(result.Operations[0].Id && result.Operations[1].Id);
            // The order must be reversed: from newest to oldest.
            UNIT_ASSERT_VALUES_EQUAL(*result.Operations[0].Id, operations[1]->GetId());
            UNIT_ASSERT_VALUES_EQUAL(*result.Operations[1].Id, operations[0]->GetId());
        }
    }

    Y_UNIT_TEST(UpdateOperationParameters)
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

        UNIT_ASSERT_VALUES_EQUAL(operation->GetAttributes().Suspended, true);

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

        UNIT_ASSERT_VALUES_EQUAL(operation->GetAttributes().Suspended, false);
    }

    Y_UNIT_TEST(SuspendResume_OperationMethod)
    {
        TestSuspendResume(true);
    }

    Y_UNIT_TEST(SuspendResume_ClientMethod)
    {
        TestSuspendResume(false);
    }

    Y_UNIT_TEST(BatchOperationControl)
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

        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::InProgress);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::InProgress);
        UNIT_ASSERT_VALUES_EQUAL(op3->GetBriefState(), EOperationBriefState::InProgress);
        batchRequest->ExecuteBatch();

        // Check that there are no errors
        abortResult.GetValue();
        completeResult.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Aborted);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Completed);

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

} // Y_UNIT_TEST_SUITE(OperationCommands)

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(JobCommands)
{

    Y_UNIT_TEST(GetJob)
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
        UNIT_ASSERT_VALUES_EQUAL(jobs.size(), 1);
        UNIT_ASSERT(jobs.front().Id);
        auto jobId = *jobs.front().Id;

        for (const auto& job : {client->GetJob(op->GetId(), jobId), op->GetJob(jobId)}) {
            UNIT_ASSERT_VALUES_EQUAL(job.Id, jobId);
            UNIT_ASSERT_VALUES_EQUAL(job.State, EJobState::Completed);
            UNIT_ASSERT_VALUES_EQUAL(job.Type, EJobType::Map);

            UNIT_ASSERT(job.StartTime);
            UNIT_ASSERT(*job.StartTime > beforeStart);

            UNIT_ASSERT(job.FinishTime);
            UNIT_ASSERT(*job.FinishTime < afterFinish);
        }
    }

    Y_UNIT_TEST(ListJobs)
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
            UNIT_ASSERT_VALUES_EQUAL(result.Jobs.size(), 3);
            for (size_t index = 0; index < result.Jobs.size(); ++index) {
                const auto& job = result.Jobs[index];

                UNIT_ASSERT(job.StartTime);
                UNIT_ASSERT(*job.StartTime > beforeStart);

                UNIT_ASSERT(job.FinishTime);
                UNIT_ASSERT(*job.FinishTime < afterFinish);

                UNIT_ASSERT(job.Type);
                UNIT_ASSERT_VALUES_EQUAL(*job.Type, EJobType::PartitionMap);

                UNIT_ASSERT(job.State);

                if (index == result.Jobs.size() - 1) {
                    UNIT_ASSERT_VALUES_EQUAL(*job.State, EJobState::Failed);

                    UNIT_ASSERT_VALUES_EQUAL(job.StderrSize.GetOrElse(0), 0);

                    UNIT_ASSERT(job.Error);
                    UNIT_ASSERT(job.Error->ContainsErrorCode(1205));
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(*job.State, EJobState::Completed);
                }

                UNIT_ASSERT(job.BriefStatistics);
            }
        }
    }

    Y_UNIT_TEST(GetJobInput)
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
                << "/orchid/job_controller/active_jobs/scheduler/" << *job.Id << "/job_phase";
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
        UNIT_ASSERT_VALUES_EQUAL(jobs.size(), 1);
        UNIT_ASSERT(jobs.front().Id.Defined());

        auto jobInputStream = client->GetJobInput(*jobs.front().Id);
        auto reader = CreateTableReader<TNode>(jobInputStream.Get());

        TVector<TNode> readRows;
        for (; reader->IsValid(); reader->Next()) {
            readRows.push_back(reader->MoveRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(expectedRows, readRows);
    }

    Y_UNIT_TEST(GetJobStderr)
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
        UNIT_ASSERT_VALUES_EQUAL(jobs.size(), 1);
        UNIT_ASSERT(jobs.front().Id.Defined());

        auto jobStderrStream = client->GetJobStderr(op->GetId(), *jobs.front().Id);
        UNIT_ASSERT_STRING_CONTAINS(jobStderrStream->ReadAll(), expectedStderr);
    }

    Y_UNIT_TEST(GetJobFailContext)
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
        UNIT_ASSERT_VALUES_EQUAL(jobs.size(), 1);
        UNIT_ASSERT(jobs.front().Id.Defined());

        auto jobFailContextStream = client->GetJobFailContext(op->GetId(), *jobs.front().Id);
        auto reader = CreateTableReader<TNode>(jobFailContextStream.Get());

        TVector<TNode> readRows;
        for (; reader->IsValid(); reader->Next()) {
            readRows.push_back(reader->MoveRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(expectedRows, readRows);
    }

} // Y_UNIT_TEST_SUITE(JobCommands)
