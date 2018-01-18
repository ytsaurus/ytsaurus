#include "lib.h"

#include <mapreduce/yt/tests/native_new/all_types.pb.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/debug_metrics.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/library/operation_tracker/operation_tracker.h>
#include <mapreduce/yt/http/abortable_http_response.h>

#include <library/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/folder/path.h>
#include <util/system/env.h>

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

////////////////////////////////////////////////////////////////////////////////


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

////////////////////////////////////////////////////////////////////////////////


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

////////////////////////////////////////////////////////////////////////////////


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

////////////////////////////////////////////////////////////////////////////////

class TMapperThatReadsProtobufFile : public IMapper<TTableReader<TNode>, TTableWriter<TAllTypesMessage>>
{
public:
    TMapperThatReadsProtobufFile() = default;
    TMapperThatReadsProtobufFile(const TString& file)
        : File_(file)
    { }

    virtual void Do(TReader*, TWriter* writer) override {
        TIFStream stream(File_);
        auto fileReader = CreateTableReader<TAllTypesMessage>(&stream);
        for (; fileReader->IsValid(); fileReader->Next()) {
            writer->AddRow(fileReader->GetRow());
        }
    }

    Y_SAVELOAD_JOB(File_);

private:
    TString File_;
};
REGISTER_MAPPER(TMapperThatReadsProtobufFile);

////////////////////////////////////////////////////////////////////////////////

class THugeStderrMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    THugeStderrMapper() = default;
    virtual void Do(TReader*, TWriter*) override {
        TString err(1024 * 1024 * 10, 'a');
        Cerr.Write(err);
        Cerr.Flush();
        exit(1);
    }
};
REGISTER_MAPPER(THugeStderrMapper);

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

class TProtobufMapper : public IMapper<TTableReader<TAllTypesMessage>, TTableWriter<TAllTypesMessage>>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        TAllTypesMessage row;
        for (; reader->IsValid(); reader->Next()) {
            reader->MoveRow(&row);
            row.SetStringField(row.GetStringField() + " mapped");
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TProtobufMapper);

////////////////////////////////////////////////////////////////////////////////

class TJobBaseThatUsesEnv
{
public:
    TJobBaseThatUsesEnv() = default;
    TJobBaseThatUsesEnv(const TString& envName)
        : EnvName_(envName)
    { }

    void Process(TTableReader<TNode>* reader, TTableWriter<TNode>* writer) {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            TString prevValue;
            if (row.HasKey(EnvName_)) {
                prevValue = row[EnvName_].AsString();
            }
            row[EnvName_] = prevValue.append(GetEnv(EnvName_));
            writer->AddRow(row);
        }
    }

protected:
    TString EnvName_;
};

////////////////////////////////////////////////////////////////////////////////

class TMapperThatUsesEnv : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>, public TJobBaseThatUsesEnv
{
public:
    TMapperThatUsesEnv() = default;
    TMapperThatUsesEnv(const TString& envName)
        : TJobBaseThatUsesEnv(envName)
    { }

    virtual void Do(TReader* reader, TWriter* writer) override {
        TJobBaseThatUsesEnv::Process(reader, writer);
    }

    Y_SAVELOAD_JOB(EnvName_);
};

REGISTER_MAPPER(TMapperThatUsesEnv);

////////////////////////////////////////////////////////////////////////////////

class TReducerThatUsesEnv : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>, public TJobBaseThatUsesEnv
{
public:
    TReducerThatUsesEnv() = default;
    TReducerThatUsesEnv(const TString& envName)
        : TJobBaseThatUsesEnv(envName)
    { }

    virtual void Do(TReader* reader, TWriter* writer) override {
        TJobBaseThatUsesEnv::Process(reader, writer);
    }

    Y_SAVELOAD_JOB(EnvName_);
};

REGISTER_REDUCER(TReducerThatUsesEnv);

////////////////////////////////////////////////////////////////////////////////

class TMapperThatWritesCustomStatistics : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* /* reader */, TWriter* /* writer */)
    {
        WriteCustomStatistics("some/path/to/stat", std::numeric_limits<i64>::min());
        auto node = TNode()
            ("second", TNode()("second-and-half", i64(-142)))
            ("third", i64(42));
        WriteCustomStatistics(node);
    }
};
REGISTER_MAPPER(TMapperThatWritesCustomStatistics);

////////////////////////////////////////////////////////////////////////////////

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
            return client->Get(operationPath).AsMap()["total"].AsInt64();
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

    SIMPLE_UNIT_TEST(TestReadProtobufFileInJob)
    {
        auto client = CreateTestClient();

        TAllTypesMessage message;
        message.SetFixed32Field(2134242);
        message.SetSfixed32Field(422142);
        message.SetBoolField(true);
        message.SetStringField("42");
        message.SetBytesField("36 popugayev");
        message.SetEnumField(EEnum::One);
        message.MutableMessageField()->SetKey("key");
        message.MutableMessageField()->SetValue("value");

        {
            auto writer = client->CreateTableWriter<TAllTypesMessage>("//testing/input");
            writer->AddRow(message);
            writer->Finish();
        }

        auto format = TFormat::Protobuf<TAllTypesMessage>();
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TAllTypesMessage>("//testing/output")
                .MapperSpec(TUserJobSpec().AddFile(TRichYPath("//testing/input").Format(format.Config))),
            new TMapperThatReadsProtobufFile("input"));

        {
            auto reader = client->CreateTableReader<TAllTypesMessage>("//testing/output");
            UNIT_ASSERT(reader->IsValid());
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(message.GetFixed32Field(), row.GetFixed32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSfixed32Field(), row.GetSfixed32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetBoolField(), row.GetBoolField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetStringField(), row.GetStringField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetBytesField(), row.GetBytesField());
            UNIT_ASSERT_EQUAL(message.GetEnumField(), row.GetEnumField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetMessageField().GetKey(), row.GetMessageField().GetKey());
            UNIT_ASSERT_VALUES_EQUAL(message.GetMessageField().GetValue(), row.GetMessageField().GetValue());
        }
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

    SIMPLE_UNIT_TEST(TestCustomStatistics)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }
        auto operation = client->Map(
            TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output"),
            new TMapperThatWritesCustomStatistics());

        auto jobStatistics = operation->GetJobStatistics();

        auto first = jobStatistics.GetCustomStatistics("some/path/to/stat").Max();
        UNIT_ASSERT(*first == std::numeric_limits<i64>::min());

        auto second = jobStatistics.GetCustomStatistics("second/second-and-half").Max();
        UNIT_ASSERT(*second == -142);
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

    SIMPLE_UNIT_TEST(MapWithProtobuf)
    {
        auto client = CreateTestClient();
        auto inputTable = TRichYPath("//testing/input");
        auto outputTable = TRichYPath("//testing/output");
        {
            auto writer = client->CreateTableWriter<TNode>(inputTable);
            writer->AddRow(TNode()("StringField", "raz"));
            writer->AddRow(TNode()("StringField", "dva"));
            writer->AddRow(TNode()("StringField", "tri"));
            writer->Finish();
        }
        client->Map(
            TMapOperationSpec()
                .AddInput<TAllTypesMessage>(inputTable)
                .AddOutput<TAllTypesMessage>(outputTable),
            new TProtobufMapper);

        TVector<TNode> expected = {
            TNode()("StringField", "raz mapped"),
            TNode()("StringField", "dva mapped"),
            TNode()("StringField", "tri mapped"),
        };
        auto actual = ReadTable(client, outputTable.Path_);
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    SIMPLE_UNIT_TEST(JobPreffix)
    {
        auto client = CreateTestClient();
        auto inputTable = TRichYPath("//testing/input");
        auto outputTable = TRichYPath("//testing/output");
        {
            auto writer = client->CreateTableWriter<TNode>(inputTable);
            writer->AddRow(TNode()("input", "dummy"));
            writer->Finish();
        }

        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new TMapperThatUsesEnv("TEST_ENV"));
        {
            auto reader = client->CreateTableReader<TNode>(outputTable);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRow()["TEST_ENV"], "");
        }

        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new TMapperThatUsesEnv("TEST_ENV"),
            TOperationOptions().JobCommandPrefix("TEST_ENV=common "));
        {
            auto reader = client->CreateTableReader<TNode>(outputTable);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRow()["TEST_ENV"], "common");
        }

        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable)
                .MapperSpec(TUserJobSpec()
                    .JobCommandPrefix("TEST_ENV=mapper ")
                ),
            new TMapperThatUsesEnv("TEST_ENV"));
        {
            auto reader = client->CreateTableReader<TNode>(outputTable);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRow()["TEST_ENV"], "mapper");
        }

        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable)
                .MapperSpec(TUserJobSpec()
                    .JobCommandPrefix("TEST_ENV=mapper ")
                ),
            new TMapperThatUsesEnv("TEST_ENV"),
            TOperationOptions().JobCommandPrefix("TEST_ENV=common "));
        {
            auto reader = client->CreateTableReader<TNode>(outputTable);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRow()["TEST_ENV"], "mapper");
        }

        client->MapReduce(
            TMapReduceOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable)
                .ReduceBy({"input"})
                .MapperSpec(TUserJobSpec()
                    .JobCommandPrefix("TEST_ENV=mapper ")
                )
                .ReducerSpec(TUserJobSpec()
                    .JobCommandPrefix("TEST_ENV=reducer ")
                ),
            new TMapperThatUsesEnv("TEST_ENV"),
            new TReducerThatUsesEnv("TEST_ENV"),
            TOperationOptions().JobCommandPrefix("TEST_ENV=common "));
        {
            auto reader = client->CreateTableReader<TNode>(outputTable);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRow()["TEST_ENV"], "mapperreducer");
        }
    }

    SIMPLE_UNIT_TEST(AddLocalFile)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        {
            TOFStream localFile("localPath");
            localFile << "Some data\n";
            localFile.Finish();
        }

        // Expect operation to complete successfully
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output")
                .MapperSpec(TUserJobSpec().AddLocalFile("localPath", TAddLocalFileOptions().PathInJob("path/in/job"))),
            new TMapperThatChecksFile("path/in/job"));
    }

    SIMPLE_UNIT_TEST(TestFailWithNoInputOutput)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "baz"));
            writer->Finish();
        }

        {
            UNIT_ASSERT_EXCEPTION(client->Map(
                TMapOperationSpec()
                .AddInput<TNode>("//testing/input"),
                new TIdMapper), TApiUsageError);
        }

        {
            UNIT_ASSERT_EXCEPTION(client->Map(
                TMapOperationSpec()
                .AddOutput<TNode>("//testing/output"),
                new TIdMapper), TApiUsageError);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

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

    SIMPLE_UNIT_TEST(TestHugeFailWithHugeStderr)
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
            .AddOutput<TNode>("//testing/output"),
            new THugeStderrMapper,
            TOperationOptions().Wait(false));

        //expect no exception
        operation->Watch().Wait();
    }
}

////////////////////////////////////////////////////////////////////////////////


SIMPLE_UNIT_TEST_SUITE(OperationTracker)
{
    void CreateTableWithFooColumn(IClientPtr client, const TString& path)
    {
        auto writer = client->CreateTableWriter<TNode>(path);
        writer->AddRow(TNode()("foo", "baz"));
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

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

    SIMPLE_UNIT_TEST(WaitAllCompleted_OkOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncSortByFoo(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompleted();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetStatus(), EOperationStatus::OS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetStatus(), EOperationStatus::OS_COMPLETED);
    }

    SIMPLE_UNIT_TEST(WaitAllCompleted_ErrorOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncAlwaysFailingMapper(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        UNIT_ASSERT_EXCEPTION(tracker.WaitAllCompleted(), TOperationFailedError);
    }

    SIMPLE_UNIT_TEST(WaitAllCompletedOrError_OkOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncSortByFoo(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompletedOrError();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetStatus(), EOperationStatus::OS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetStatus(), EOperationStatus::OS_COMPLETED);
    }

    SIMPLE_UNIT_TEST(WaitAllCompletedOrError_ErrorOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncAlwaysFailingMapper(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompletedOrError();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetStatus(), EOperationStatus::OS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetStatus(), EOperationStatus::OS_FAILED);
    }

    SIMPLE_UNIT_TEST(WaitOneCompleted_OkOperation)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncSortByFoo(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        auto waited1 = tracker.WaitOneCompleted();
        UNIT_ASSERT(waited1);
        UNIT_ASSERT_VALUES_EQUAL(waited1->GetStatus(), OS_COMPLETED);

        auto waited2 = tracker.WaitOneCompleted();
        UNIT_ASSERT(waited2);
        UNIT_ASSERT_VALUES_EQUAL(waited2->GetStatus(), OS_COMPLETED);

        auto waited3 = tracker.WaitOneCompleted();
        UNIT_ASSERT(!waited3);
        UNIT_ASSERT_VALUES_EQUAL(TSet<IOperation*>({op1.Get(), op2.Get()}), TSet<IOperation*>({waited1.Get(), waited2.Get()}));
    }

    SIMPLE_UNIT_TEST(WaitOneCompleted_ErrorOperation)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncAlwaysFailingMapper(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        auto waitByOne = [&] {
            auto waited1 = tracker.WaitOneCompleted();
            auto waited2 = tracker.WaitOneCompleted();
        };

        UNIT_ASSERT_EXCEPTION(waitByOne(), TOperationFailedError);
    }

    SIMPLE_UNIT_TEST(WaitOneCompletedOrError_ErrorOperation)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncAlwaysFailingMapper(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        auto waited1 = tracker.WaitOneCompletedOrError();
        UNIT_ASSERT(waited1);

        auto waited2 = tracker.WaitOneCompletedOrError();
        UNIT_ASSERT(waited2);

        auto waited3 = tracker.WaitOneCompletedOrError();
        UNIT_ASSERT(!waited3);

        UNIT_ASSERT_VALUES_EQUAL(TSet<IOperation*>({op1.Get(), op2.Get()}), TSet<IOperation*>({waited1.Get(), waited2.Get()}));
        UNIT_ASSERT_VALUES_EQUAL(op1->GetStatus(), OS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetStatus(), OS_FAILED);
    }

    SIMPLE_UNIT_TEST(ConnectionErrorWhenOperationIsTracked)
    {
        TConfigSaverGuard csg;
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->EnableDebugMetrics = true;
        TConfig::Get()->RetryCount = 1;
        TConfig::Get()->ReadRetryCount = 1;
        TConfig::Get()->StartOperationRetryCount = 1;
        TConfig::Get()->WaitLockPollInterval = TDuration::MilliSeconds(0);

        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");
        auto tx = client->StartTransaction();

        auto op = tx->Map(
            TMapOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output"),
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
}

////////////////////////////////////////////////////////////////////////////////
