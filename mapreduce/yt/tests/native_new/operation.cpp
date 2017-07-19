#include "lib.h"

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/helpers.h>

#include <library/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/folder/path.h>

using namespace NYT;
using namespace NYT::NTesting;

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
                operationId = e.Id;
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
}

////////////////////////////////////////////////////////////////////
