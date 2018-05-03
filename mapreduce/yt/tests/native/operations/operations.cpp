#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/tests/native/proto_lib/all_types.pb.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/debug_metrics.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/finally_guard.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <mapreduce/yt/library/lazy_sort/lazy_sort.h>
#include <mapreduce/yt/library/operation_tracker/operation_tracker.h>

#include <mapreduce/yt/util/wait_for_tablets_state.h>

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

void CreateTableWithFooColumn(IClientPtr client, const TString& path)
{
    auto writer = client->CreateTableWriter<TNode>(path);
    writer->AddRow(TNode()("foo", "baz"));
    writer->AddRow(TNode()("foo", "bar"));
    writer->Finish();
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
        WriteCustomStatistics("another/path/to/stat\\/with\\/escaping", i64(43));
        WriteCustomStatistics("ambiguous/path", i64(7331));
        WriteCustomStatistics("ambiguous\\/path", i64(1337));
    }
};
REGISTER_MAPPER(TMapperThatWritesCustomStatistics);

////////////////////////////////////////////////////////////////////////////////

class TReducerThatSumsFirstThreeValues : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        i64 sum = 0;
        auto key = reader->GetRow()["key"];
        for (int i = 0; i < 3; ++i) {
            sum += reader->GetRow()["value"].AsInt64();
            reader->Next();
            if (!reader->IsValid()) {
                break;
            }
        }
        writer->AddRow(TNode()("key", key)("sum", sum));
    }
};
REGISTER_REDUCER(TReducerThatSumsFirstThreeValues);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(Operations)
{
    Y_UNIT_TEST(IncorrectTableId)
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

    Y_UNIT_TEST(MaxFailedJobCount)
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

    Y_UNIT_TEST(FailOnJobRestart)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        TOperationId operationId;
        try {
            client->Map(
                TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output")
                .FailOnJobRestart(true)
                .MaxFailedJobCount(3),
                new TAlwaysFailingMapper);
            UNIT_FAIL("Operation expected to fail");
        } catch (const TOperationFailedError& e) {
            operationId = e.GetOperationId();
        }

        auto failedJobs = client->Get(TStringBuilder() << "//sys/operations/" << operationId << "/@brief_progress/jobs/failed");
        UNIT_ASSERT_VALUES_EQUAL(failedJobs.AsInt64(), 1);
    }

    Y_UNIT_TEST(StderrTablePath)
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

    Y_UNIT_TEST(CreateDebugOutputTables)
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

    Y_UNIT_TEST(CreateOutputTables)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        // Output table does not exist => operation should fail.
        UNIT_ASSERT_EXCEPTION(
            client->Map(
                TMapOperationSpec().CreateOutputTables(false)
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output")
                .StderrTablePath("//testing/stderr"),
                new TMapperThatWritesStderr),
            TOperationFailedError);

        client->Create("//testing/output", NT_TABLE);

        // Output table exists => should complete ok.
        client->Map(
            TMapOperationSpec().CreateDebugOutputTables(false)
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .StderrTablePath("//testing/stderr"),
            new TMapperThatWritesStderr);
    }

    Y_UNIT_TEST(JobCount)
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
            auto result = client->Get("//sys/operations/" + GetGuidAsString(operationId) + "/@brief_progress/jobs/completed");
            return (result.IsInt64() ? result : result["total"]).AsInt64();
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

    Y_UNIT_TEST(TestFetchTable)
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

    Y_UNIT_TEST(TestFetchTableRange)
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

    Y_UNIT_TEST(TestReadProtobufFileInJob)
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

    Y_UNIT_TEST(TestGetOperationStatus_Completed)
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

        while (operation->GetState() == EOperationState::InProgress) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Completed);
        UNIT_ASSERT(operation->GetError().Empty());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Completed);
        UNIT_ASSERT(operation->GetError().Empty());
    }

    Y_UNIT_TEST(TestGetOperationStatus_Failed)
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

        while (operation->GetState() == EOperationState::InProgress) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Failed);
        UNIT_ASSERT(operation->GetError().Defined());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Failed);
        UNIT_ASSERT(operation->GetError().Defined());
    }

    Y_UNIT_TEST(TestGetOperationStatistics)
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

    Y_UNIT_TEST(TestCustomStatistics)
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

        auto another = jobStatistics.GetCustomStatistics("another/path/to/stat\\/with\\/escaping").Max();
        UNIT_ASSERT(*another == 43);

        auto unescaped = jobStatistics.GetCustomStatistics("ambiguous/path").Max();
        UNIT_ASSERT(*unescaped == 7331);

        auto escaped = jobStatistics.GetCustomStatistics("ambiguous\\/path").Max();
        UNIT_ASSERT(*escaped == 1337);
    }

    Y_UNIT_TEST(GetBriefProgress)
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

    void MapWithProtobuf(bool useDeprecatedAddInput, bool useClientProtobuf)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->UseClientProtobuf = useClientProtobuf;

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
        TMapOperationSpec spec;
        if (useDeprecatedAddInput) {
            spec
                .AddProtobufInput_VerySlow_Deprecated(inputTable)
                .AddProtobufOutput_VerySlow_Deprecated(outputTable);
        } else {
            spec
                .AddInput<TAllTypesMessage>(inputTable)
                .AddOutput<TAllTypesMessage>(outputTable);
        }

        client->Map(spec, new TProtobufMapper);

        TVector<TNode> expected = {
            TNode()("StringField", "raz mapped"),
            TNode()("StringField", "dva mapped"),
            TNode()("StringField", "tri mapped"),
        };
        auto actual = ReadTable(client, outputTable.Path_);
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(ProtobufMap_NativeProtobuf)
    {
        MapWithProtobuf(false, false);
    }

    Y_UNIT_TEST(ProtobufMap_ClientProtobuf)
    {
        MapWithProtobuf(false, true);
    }

    Y_UNIT_TEST(ProtobufMap_Input_VerySlow_Deprecated_NativeProtobuf)
    {
        MapWithProtobuf(true, false);
    }

    Y_UNIT_TEST(ProtobufMap_Input_VerySlow_Deprecated_ClientProtobuf)
    {
        MapWithProtobuf(true, true);
    }

    Y_UNIT_TEST(JobPrefix)
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

    Y_UNIT_TEST(AddLocalFile)
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

    Y_UNIT_TEST(TestFailWithNoInputOutput)
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

    Y_UNIT_TEST(MaxOperationCountExceeded)
    {
        TConfigSaverGuard csg;
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->StartOperationRetryCount = 3;
        TConfig::Get()->StartOperationRetryInterval = TDuration::MilliSeconds(0);

        auto client = CreateTestClient();

        size_t maxOperationCount = 1;
        client->Create("//sys/pools/research/testing", NT_MAP, TCreateOptions().IgnoreExisting(true).Recursive(true));
        client->Set("//sys/pools/research/testing/@max_operation_count", maxOperationCount);

        CreateTableWithFooColumn(client, "//testing/input");

        TVector<IOperationPtr> operations;

        NYT::NDetail::TFinallyGuard finally([&]{
            for (auto& operation : operations) {
                operation->AbortOperation();
            }
        });

        try {
            for (size_t i = 0; i < maxOperationCount + 1; ++i) {
                operations.push_back(client->Map(
                    TMapOperationSpec()
                        .AddInput<TNode>("//testing/input")
                        .AddOutput<TNode>("//testing/output_" + ToString(i)),
                    new TSleepingMapper(TDuration::Seconds(3600)),
                    TOperationOptions()
                        .Spec(TNode()("pool", "testing"))
                        .Wait(false)));
            }
            UNIT_FAIL("Too many Map's must have been failed");
        } catch (const TErrorResponse& error) {
            // It's OK
        }
    }

    Y_UNIT_TEST(NetworkProblems)
    {
        TConfigSaverGuard csg;
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->StartOperationRetryCount = 3;
        TConfig::Get()->StartOperationRetryInterval = TDuration::MilliSeconds(0);

        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        try {
            auto outage = TAbortableHttpResponse::StartOutage("/map");
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output_1"),
                new TIdMapper());
            UNIT_FAIL("Start operation must have been failed");
        } catch (const TAbortedForTestPurpose&) {
            // It's OK
        }
        {
            auto outage = TAbortableHttpResponse::StartOutage("/map", TConfig::Get()->StartOperationRetryCount - 1);
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output_2"),
                new TIdMapper());
        }
    }

    void TestJobNodeReader(ENodeReaderFormat nodeReaderFormat, bool strictSchema)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = nodeReaderFormat;

        auto client = CreateTestClient();
        TString inputPath = "//testing/input";
        TString outputPath = "//testing/input";
        NYT::NDetail::TFinallyGuard finally([&]{
            client->Remove(inputPath, TRemoveOptions().Force(true));
        });

        auto row = TNode()
            ("int64", 1 - (1LL << 62))
            ("int16", 42 - (1 << 14))
            ("uint64", 1ULL << 63)
            ("uint16", 1U << 15)
            ("boolean", true)
            ("double", 1.4242e42)
            ("string", "Just a string");
        auto schema = TTableSchema().Strict(strictSchema);
        for (const auto& p : row.AsMap()) {
            EValueType type;
            Deserialize(type, p.first);
            schema.AddColumn(TColumnSchema().Name(p.first).Type(type));
        }
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputPath).Schema(schema));
            writer->AddRow(row);
            writer->Finish();
        }

        client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(inputPath)
            .AddOutput<TNode>(outputPath)
            .MaxFailedJobCount(1),
            new TIdMapper());

        auto reader = client->CreateTableReader<TNode>(outputPath);
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), row);
    }

    Y_UNIT_TEST(JobNodeReader_Skiff_Strict)
    {
        TestJobNodeReader(ENodeReaderFormat::Skiff, true);
    }
    Y_UNIT_TEST(JobNodeReader_Skiff_NonStrict)
    {
        UNIT_ASSERT_EXCEPTION(TestJobNodeReader(ENodeReaderFormat::Skiff, false), yexception);
    }
    Y_UNIT_TEST(JobNodeReader_Auto_Strict)
    {
        TestJobNodeReader(ENodeReaderFormat::Auto, true);
    }
    Y_UNIT_TEST(JobNodeReader_Auto_NonStrict)
    {
        TestJobNodeReader(ENodeReaderFormat::Auto, false);
    }
    Y_UNIT_TEST(JobNodeReader_Yson_Strict)
    {
        TestJobNodeReader(ENodeReaderFormat::Yson, true);
    }
    Y_UNIT_TEST(JobNodeReader_Yson_NonStrict)
    {
        TestJobNodeReader(ENodeReaderFormat::Yson, false);
    }

    void TestIncompleteReducer(ENodeReaderFormat nodeReaderFormat)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = nodeReaderFormat;
        auto client = CreateTestClient();
        auto inputPath = TRichYPath("//testing/input")
            .Schema(TTableSchema()
                .Strict(true)
                .AddColumn(TColumnSchema().Name("key").Type(VT_INT64).SortOrder(SO_ASCENDING))
                .AddColumn(TColumnSchema().Name("value").Type(VT_INT64)));
        auto outputPath = TRichYPath("//testing/output");
        {
            auto writer = client->CreateTableWriter<TNode>(inputPath);
            for (auto key : {1, 2,2, 3,3,3, 4,4,4,4, 5,5,5,5,5}) {
                writer->AddRow(TNode()("key", key)("value", i64(1)));
            }
        }
        client->Reduce(
            TReduceOperationSpec()
                .ReduceBy({"key"})
                .AddInput<TNode>(inputPath)
                .AddOutput<TNode>(outputPath),
            new TReducerThatSumsFirstThreeValues());
        {
            TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson;
            auto reader = client->CreateTableReader<TNode>(outputPath);
            TVector<i64> expectedValues = {1,2,3,3,3};
            for (size_t index = 0; index < expectedValues.size(); ++index) {
                UNIT_ASSERT(reader->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(),
                    TNode()
                        ("key", static_cast<i64>(index + 1))
                        ("sum", expectedValues[index]));
                reader->Next();
            }
            UNIT_ASSERT(!reader->IsValid());
        }
    }

    Y_UNIT_TEST(IncompleteReducer_Yson)
    {
        TestIncompleteReducer(ENodeReaderFormat::Yson);
    }

    Y_UNIT_TEST(IncompleteReducer_Skiff)
    {
        TestIncompleteReducer(ENodeReaderFormat::Skiff);
    }

    Y_UNIT_TEST(SkiffForDynamicTables)
    {
        TConfigSaverGuard configGuard;
        TTabletFixture fixture;
        auto client = fixture.Client();
        auto schema = TNode()
            .Add(TNode()("name", "key")("type", "string"))
            .Add(TNode()("name", "value")("type", "int64"));
        const auto inputPath = "//testing/input";
        const auto outputPath = "//testing/output";
        client->Create(inputPath, NT_TABLE, TCreateOptions().Attributes(
            TNode()("dynamic", true)("schema", schema)));
        client->MountTable(inputPath);
        WaitForTabletsState(client, inputPath, TS_MOUNTED, TWaitForTabletsStateOptions()
            .Timeout(TDuration::Seconds(30))
            .CheckInterval(TDuration::MilliSeconds(50)));
        client->InsertRows(inputPath, {TNode()("key", "key")("value", 33)});

        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputPath)
                .AddOutput<TNode>(outputPath),
            new TIdMapper);

        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;
        UNIT_ASSERT_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(inputPath)
                    .AddOutput<TNode>(outputPath),
                new TIdMapper),
            yexception);
    }

    Y_UNIT_TEST(LockConflictWhileTouchingCachedFiles)
    {
        auto client = CreateTestClient();
        client->Create("//testing/file_storage", NT_MAP);

        CreateTableWithFooColumn(client, "//testing/input");

        { // Load files to cache
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output_1"),
                new TIdMapper(),
                TOperationOptions()
                    .FileStorage("//testing/file_storage"));
        }

        auto tx1 = client->StartTransaction();
        { // Now operation will touch files using tx1
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output_1"),
                new TIdMapper(),
                TOperationOptions()
                    .FileStorage("//testing/file_storage")
                    .FileStorageTransactionId(tx1->GetId()));
        }

        auto tx2 = client->StartTransaction();
        { // Second operation will touch files using tx2, but they are still holded by tx1
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output_1"),
                new TIdMapper(),
                TOperationOptions()
                    .FileStorage("//testing/file_storage")
                    .FileStorageTransactionId(tx2->GetId()));
        }
    }

    Y_UNIT_TEST(LazySort)
    {
        auto client = CreateTestClient();
        TString inputTable = "//testing/table";
        auto initialSortedBy = TKeyColumns().Add("key1").Add("key2").Add("key3");

        auto getSortedBy = [&](const TString& table) {
            TKeyColumns columns;
            auto sortedBy = client->Get(table + "/@sorted_by");
            for (const auto& node : sortedBy.AsList()) {
                columns.Add(node.AsString());
            }
            return columns;
        };

        auto getType = [&](const IOperationPtr& operation) {
            // TODO(levysotsky) Use client->GetOperation() when it's ready (YT-8606)
            return client->Get("//sys/operations/" + GetGuidAsString(operation->GetId()) + "/@operation_type").AsString();
        };

        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputTable).SortedBy(initialSortedBy));
            writer->AddRow(TNode()("key1", "a")("key2", "b")("key3", "c")("value", "x"));
            writer->AddRow(TNode()("key1", "a")("key2", "b")("key3", "d")("value", "xx"));
            writer->AddRow(TNode()("key1", "a")("key2", "c")("key3", "a")("value", "xxx"));
            writer->AddRow(TNode()("key1", "b")("key2", "a")("key3", "a")("value", "xxxx"));
            writer->Finish();
        }

        {
            auto prefixColumns = TKeyColumns().Add("key1").Add("key2");
            TString outputTable = "//testing/output";
            auto operation = LazySort(
                client,
                TSortOperationSpec()
                    .AddInput(inputTable)
                    .AddInput(inputTable)
                    .Output(outputTable)
                    .SortBy(prefixColumns));

            UNIT_ASSERT_UNEQUAL(operation, nullptr);
            // It must be merge because input tables are already sorted
            UNIT_ASSERT_VALUES_EQUAL(getType(operation), "merge");
            UNIT_ASSERT_VALUES_EQUAL(getSortedBy(outputTable).Parts_, prefixColumns.Parts_);
            UNIT_ASSERT_VALUES_EQUAL(
                client->Get(outputTable + "/@row_count").AsInt64(),
                2 * client->Get(inputTable + "/@row_count").AsInt64());
        }
        {
            auto nonPrefixColumns = TKeyColumns().Add("key2").Add("key3");
            TString outputTable = "//testing/output";
            auto operation = LazySort(
                client,
                TSortOperationSpec()
                    .AddInput(inputTable)
                    .Output(outputTable)
                    .SortBy(nonPrefixColumns));
            UNIT_ASSERT_UNEQUAL(operation, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(getType(operation), "sort");
            UNIT_ASSERT_VALUES_EQUAL(getSortedBy(outputTable).Parts_, nonPrefixColumns.Parts_);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(OperationWatch)
{
    Y_UNIT_TEST(SimpleOperationWatch)
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Completed);
        UNIT_ASSERT(operation->GetError().Empty());
    }

    Y_UNIT_TEST(FailedOperationWatch)
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Failed);
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Aborted);
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetState(), EOperationState::Completed);
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

    Y_UNIT_TEST(GetFailedJobInfo_GlobalClient)
    {
        TestGetFailedJobInfoImpl(CreateTestClient());
    }

    Y_UNIT_TEST(GetFailedJobInfo_Transaction)
    {
        TestGetFailedJobInfoImpl(CreateTestClient()->StartTransaction());
    }

    Y_UNIT_TEST(GetBriefProgress)
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

    Y_UNIT_TEST(TestHugeFailWithHugeStderr)
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
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncSortByFoo(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompleted();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetState(), EOperationState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetState(), EOperationState::Completed);
    }

    Y_UNIT_TEST(WaitAllCompleted_ErrorOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncAlwaysFailingMapper(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        UNIT_ASSERT_EXCEPTION(tracker.WaitAllCompleted(), TOperationFailedError);
    }

    Y_UNIT_TEST(WaitAllCompletedOrError_OkOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncSortByFoo(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompletedOrError();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetState(), EOperationState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetState(), EOperationState::Completed);
    }

    Y_UNIT_TEST(WaitAllCompletedOrError_ErrorOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, "//testing/input", "//testing/output1");
        auto op2 = AsyncAlwaysFailingMapper(client, "//testing/input", "//testing/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompletedOrError();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetState(), EOperationState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetState(), EOperationState::Failed);
    }

    Y_UNIT_TEST(WaitOneCompleted_OkOperation)
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
        UNIT_ASSERT_VALUES_EQUAL(waited1->GetState(), EOperationState::Completed);

        auto waited2 = tracker.WaitOneCompleted();
        UNIT_ASSERT(waited2);
        UNIT_ASSERT_VALUES_EQUAL(waited2->GetState(), EOperationState::Completed);

        auto waited3 = tracker.WaitOneCompleted();
        UNIT_ASSERT(!waited3);
        UNIT_ASSERT_VALUES_EQUAL(TSet<IOperation*>({op1.Get(), op2.Get()}), TSet<IOperation*>({waited1.Get(), waited2.Get()}));
    }

    Y_UNIT_TEST(WaitOneCompleted_ErrorOperation)
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

    Y_UNIT_TEST(WaitOneCompletedOrError_ErrorOperation)
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetState(), EOperationState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetState(), EOperationState::Failed);
    }

    Y_UNIT_TEST(ConnectionErrorWhenOperationIsTracked)
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
