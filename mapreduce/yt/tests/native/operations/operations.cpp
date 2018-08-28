#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/tests/native/proto_lib/all_types.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/row.pb.h>

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
#include <util/system/fs.h>
#include <util/system/mktemp.h>
#include <util/system/tempfile.h>
#include <util/thread/pool.h>

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

class TIdAndKvSwapMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override {
        for (; reader->IsValid(); reader->Next()) {
            const auto& node = reader->GetRow();
            TNode swapped;
            swapped["key"] = node["value"];
            swapped["value"] = node["key"];
            writer->AddRow(node, 0);
            writer->AddRow(swapped, 1);
        }
    }
};
REGISTER_MAPPER(TIdAndKvSwapMapper);

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

class TSplitGoodUrlMapper : public IMapper<TTableReader<TUrlRow>, TTableWriter<::google::protobuf::Message>>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto urlRow = reader->GetRow();
            if (urlRow.GetHttpCode() == 200) {
                TGoodUrl goodUrl;
                goodUrl.SetUrl(urlRow.GetHost() + urlRow.GetPath());
                writer->AddRow(goodUrl, 1);
            }
            writer->AddRow(urlRow, 0);
        }
    }
};
REGISTER_MAPPER(TSplitGoodUrlMapper);

////////////////////////////////////////////////////////////////////////////////

class TCountHttpCodeTotalReducer : public IReducer<TTableReader<TUrlRow>, TTableWriter<THostRow>>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        THostRow hostRow;
        i32 total = 0;
        for (; reader->IsValid(); reader->Next()) {
            auto urlRow = reader->GetRow();
            if (!hostRow.HasHost()) {
                hostRow.SetHost(urlRow.GetHost());
            }
            total += urlRow.GetHttpCode();
        }
        hostRow.SetHttpCodeTotal(total);
        writer->AddRow(hostRow);
    }
};
REGISTER_REDUCER(TCountHttpCodeTotalReducer);

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

class TVanillaAppendingToFile : public IVanillaJob
{
public:
    TVanillaAppendingToFile() = default;
    TVanillaAppendingToFile(TStringBuf fileName, TStringBuf message)
        : FileName_(fileName)
        , Message_(message)
    { }

    void Do() override
    {
        TFile file(FileName_, EOpenModeFlag::ForAppend);
        file.Write(~Message_, +Message_);
    }

    Y_SAVELOAD_JOB(FileName_, Message_);

private:
    TString FileName_;
    TString Message_;
};
REGISTER_VANILLA_JOB(TVanillaAppendingToFile);

////////////////////////////////////////////////////////////////////////////////

class TFailingVanilla : public IVanillaJob
{
public:
    void Do() override
    {
        Cerr << "I'm writing to stderr, then gonna fail" << Endl;
        ::exit(1);
    }
};
REGISTER_VANILLA_JOB(TFailingVanilla);

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

class TMapperThatNumbersRows : public IMapper<TNodeReader, TNodeWriter>
{
public:
    void Do(TReader* reader, TWriter* writer) {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            row["INDEX"] = reader->GetRowIndex();
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TMapperThatNumbersRows);

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

    Y_UNIT_TEST(EnableKeyGuarantee)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/input")
                    .Schema(TTableSchema()
                        .Strict(true)
                        .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))));
            writer->AddRow(TNode()("key", "foo"));
            writer->Finish();
        }

        auto op = client->Reduce(
            TReduceOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .ReduceBy("key")
            .EnableKeyGuarantee(false),
            new TIdReducer);
        auto spec = client->GetOperation(op->GetId()).Spec;
        UNIT_ASSERT_EQUAL((*spec)["enable_key_guarantee"].AsBool(), false);
    }

    Y_UNIT_TEST(OrderedMapReduce)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/input")
                    .Schema(TTableSchema()
                        .Strict(true)
                        .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))));
            writer->AddRow(TNode()("key", "foo"));
            writer->Finish();
        }

        auto op = client->MapReduce(
            TMapReduceOperationSpec()
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output")
            .ReduceBy("key")
            .Ordered(true),
            new TIdMapper,
            new TIdReducer);
        auto spec = client->GetOperation(op->GetId()).Spec;
        UNIT_ASSERT_EQUAL((*spec)["ordered"].AsBool(), true);
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
        UNIT_ASSERT(reader->GetRow()["data"].AsString().Contains("PYSHCH\n"));
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
        UNIT_ASSERT_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output")
                    .StderrTablePath("//testing/stderr"),
                new TMapperThatWritesStderr,
                TOperationOptions()
                    .CreateDebugOutputTables(false)),
            TOperationFailedError);

        client->Create("//testing/stderr", NT_TABLE);

        // stderr table exists => should pass
        UNIT_ASSERT_NO_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output")
                    .StderrTablePath("//testing/stderr"),
                new TMapperThatWritesStderr,
                TOperationOptions()
                    .CreateDebugOutputTables(false)));
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
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output")
                    .StderrTablePath("//testing/stderr"),
                new TMapperThatWritesStderr,
                TOperationOptions()
                    .CreateOutputTables(false)),
            TOperationFailedError);

        client->Create("//testing/output", NT_TABLE);

        // Output table exists => should complete ok.
        UNIT_ASSERT_NO_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output")
                    .StderrTablePath("//testing/stderr"),
                new TMapperThatWritesStderr,
                TOperationOptions()
                    .CreateOutputTables(false)));

        // Inputs not checked => we get TApiUsageError.
        UNIT_ASSERT_EXCEPTION(
            client->Sort(
                TSortOperationSpec()
                    .AddInput("//testing/nonexistent-input")
                    .Output("//testing/nonexistent-input")),
            TApiUsageError);

        // Inputs are not checked => we get an error response from the server.
        UNIT_ASSERT_EXCEPTION(
            client->Sort(
                TSortOperationSpec()
                    .AddInput("//testing/nonexistent-input")
                    .Output("//testing/nonexistent-input"),
                TOperationOptions()
                    .CreateOutputTables(false)),
            TOperationFailedError);
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

        while (operation->GetBriefState() == EOperationBriefState::InProgress) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT(operation->GetError().Empty());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Completed);
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

        while (operation->GetBriefState() == EOperationBriefState::InProgress) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Failed);
        UNIT_ASSERT(operation->GetError().Defined());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Failed);
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

    Y_UNIT_TEST(MapReduceMapOutput)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/input");
            writer->AddRow(TNode()("key", "foo")("value", "bar"));
            writer->Finish();
        }

        client->MapReduce(
            TMapReduceOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddMapOutput<TNode>("//testing/map_output")
                .AddOutput<TNode>("//testing/output")
                .ReduceBy({"key"}),
            new TIdAndKvSwapMapper,
            new TIdReducer);

        UNIT_ASSERT_VALUES_EQUAL(
            ReadTable(client, "//testing/output"),
            TVector<TNode>{TNode()("key", "foo")("value", "bar")});

        UNIT_ASSERT_VALUES_EQUAL(
            ReadTable(client, "//testing/map_output"),
            TVector<TNode>{TNode()("key", "bar")("value", "foo")});
    }

    Y_UNIT_TEST(MapReduceMapOutputProtobuf)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TUrlRow>("//testing/input");
            TUrlRow row;
            row.SetHost("http://example.com");
            row.SetPath("/index.php");
            row.SetHttpCode(200);
            writer->AddRow(row);
            writer->Finish();
        }

        client->MapReduce(
            TMapReduceOperationSpec()
                .AddInput<TUrlRow>("//testing/input")
                .HintMapOutput<TUrlRow>()
                .AddMapOutput<TGoodUrl>("//testing/map_output")
                .AddOutput<THostRow>("//testing/output")
                .ReduceBy({"key"}),
            new TSplitGoodUrlMapper,
            new TCountHttpCodeTotalReducer);
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
            UNIT_FAIL("Too many Maps must have been failed");
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

    Y_UNIT_TEST(TestSkiffOperationHint)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;

        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/input")
                .Schema(TTableSchema()
                    .Strict(true)
                    .AddColumn(TColumnSchema().Name("key").Type(VT_STRING))
                    .AddColumn(TColumnSchema().Name("value").Type(VT_STRING))));

            writer->AddRow(TNode()("key", "foo")("value", TNode::CreateEntity()));
            writer->Finish();
        }

        client->Map(
            TMapOperationSpec()
            .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output"),
            new TIdMapper);

        const std::vector<TNode> expected = {TNode()("key", "foo")};
        auto reader = client->CreateTableReader<TNode>("//testing/output");
        std::vector<TNode> actual;
        for (; reader->IsValid(); reader->Next()) {
            actual.push_back(reader->GetRow());
        }
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(TestSkiffOperationHintConfigurationConflict)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;

        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/input")
                .Schema(TTableSchema()
                    .Strict(true)
                    .AddColumn(TColumnSchema().Name("key").Type(VT_STRING))
                    .AddColumn(TColumnSchema().Name("value").Type(VT_STRING))));
            writer->AddRow(TNode()("key", "foo")("value", TNode::CreateEntity()));
            writer->Finish();
        }

        UNIT_ASSERT_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output"),
                new TIdMapper),
            TApiUsageError);
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

    void TestRowIndices(ENodeReaderFormat nodeReaderFormat)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = nodeReaderFormat;

        auto client = CreateTestClient();
        TYPath inputTable = "//testing/input";
        TYPath outputTable = "//testing/output";

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath(inputTable)
                    .Schema(TTableSchema().AddColumn("foo", VT_INT64)));
            for (size_t i = 0; i < 10; ++i) {
                writer->AddRow(TNode()("foo", i));
            }
            writer->Finish();
        }

        client->MapReduce(
            TMapReduceOperationSpec()
                .AddInput<TNode>(TRichYPath(inputTable)
                    .AddRange(TReadRange()
                        .LowerLimit(TReadLimit().RowIndex(3))
                        .UpperLimit(TReadLimit().RowIndex(8))))
                .AddOutput<TNode>(outputTable)
                .SortBy(TKeyColumns().Add("foo")),
            new TMapperThatNumbersRows,
            new TIdReducer);

        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson;
        {
            auto reader = client->CreateTableReader<TNode>(outputTable);
            for (int i = 3; i < 8; ++i) {
                UNIT_ASSERT(reader->IsValid());
                UNIT_ASSERT_EQUAL(reader->GetRow(), TNode()("foo", i)("INDEX", static_cast<ui64>(i)));
                reader->Next();
            }
            UNIT_ASSERT(!reader->IsValid());
        }
    }

    Y_UNIT_TEST(RowIndices_Yson)
    {
        TestRowIndices(ENodeReaderFormat::Yson);
    }

    Y_UNIT_TEST(RowIndices_Skiff)
    {
        TestRowIndices(ENodeReaderFormat::Skiff);
    }

    Y_UNIT_TEST(SkiffForInputQuery)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;

        auto client = CreateTestClient();
        TYPath inputTable = "//testing/input";
        TYPath outputTable = "//testing/output";

        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputTable)
                .Schema(TTableSchema()
                    .AddColumn("foo", VT_INT64)
                    .AddColumn("bar", VT_INT64)));
            for (size_t i = 0; i < 10; ++i) {
                writer->AddRow(TNode()("foo", i)("bar", 10 * i));
            }
            writer->Finish();
        }

        UNIT_ASSERT_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(inputTable)
                    .AddOutput<TNode>(outputTable),
                new TMapperThatNumbersRows,
                TOperationOptions()
                    .Spec(TNode()("input_query", "foo AS foo WHERE foo > 5"))),
            TApiUsageError);
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
        UNIT_ASSERT_NO_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(inputPath)
                    .AddOutput<TNode>(outputPath),
                new TIdMapper));

        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;
        UNIT_ASSERT_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(inputPath)
                    .AddOutput<TNode>(outputPath),
                new TIdMapper),
            yexception);
    }

    Y_UNIT_TEST(FileCacheModes)
    {
        auto client = CreateTestClient();
        client->Create("//testing/file_storage", NT_MAP);
        CreateTableWithFooColumn(client, "//testing/input");

        TTempFile tempFile("/tmp/yt-cpp-api-testing");
        {
            TOFStream os(tempFile.Name());
            // Create a file with unique contents to get cache miss
            os << CreateGuidAsString();
        }

        auto tx = client->StartTransaction();

        UNIT_ASSERT_EXCEPTION(
            tx->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output")
                    .MapperSpec(TUserJobSpec()
                        .AddLocalFile(tempFile.Name())),
                new TIdMapper,
                TOperationOptions()
                    .FileStorage("//testing/file_storage")
                    .FileStorageTransactionId(tx->GetId())),
            TApiUsageError);

        UNIT_ASSERT_NO_EXCEPTION(
            tx->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output")
                    .MapperSpec(TUserJobSpec()
                        .AddLocalFile(tempFile.Name())),
                new TIdMapper,
                TOperationOptions()
                    .FileStorage("//testing/file_storage")
                    .FileStorageTransactionId(tx->GetId())
                    .FileCacheMode(TOperationOptions::EFileCacheMode::CachelessRandomPathUpload)));
    }

    Y_UNIT_TEST(RetryLockConflict)
    {
        auto client = CreateTestClient();
        CreateTableWithFooColumn(client, "//testing/input");

        TTempFile tempFile("/tmp/yt-cpp-api-testing");
        {
            TOFStream os(tempFile.Name());
            // Create a file with unique contents to get cache miss
            os << CreateGuidAsString();
        }

        auto runMap = [&] {
            auto tx = client->StartTransaction();
            tx->Map(
                TMapOperationSpec()
                    .AddInput<TNode>("//testing/input")
                    .AddOutput<TNode>("//testing/output_" + CreateGuidAsString())
                    .MapperSpec(TUserJobSpec()
                        .AddLocalFile(tempFile.Name())),
                new TAlwaysFailingMapper, // No exception here because of '.Wait(false)'.
                TOperationOptions()
                    .Wait(false));
        };

        auto threadPool = SystemThreadPool();
        TVector<TAutoPtr<IThreadPool::IThread>> threads;
        // Run many concurrent threads to get lock conflict in 'put_file_to_cache'
        // with high probability.
        for (int i = 0; i < 10; ++i) {
            threads.push_back(threadPool->Run(runMap));
        }
        for (auto& t : threads) {
            t->Join();
        }
    }

    Y_UNIT_TEST(Vanilla)
    {
        auto client = CreateTestClient();

        TString fileName = MakeTempName(NFs::CurrentWorkingDirectory().c_str());
        TString message = "Hello world!";
        ui64 firstJobCount = 2, secondJobCount = 3;

        client->RunVanilla(TVanillaOperationSpec()
            .AddTask(TVanillaTask()
                .Name("first")
                .Job(new TVanillaAppendingToFile(fileName, message))
                .JobCount(firstJobCount))
            .AddTask(TVanillaTask()
                .Name("second")
                .Job(new TVanillaAppendingToFile(fileName, message))
                .JobCount(secondJobCount)));

        TIFStream stream(fileName);
        UNIT_ASSERT_VALUES_EQUAL(stream.ReadAll().size(), (firstJobCount + secondJobCount) * message.size());
    }

    Y_UNIT_TEST(FailingVanilla)
    {
        auto client = CreateTestClient();

        TYPath stderrPath = "//testing/stderr";

        client->Create(stderrPath, NT_TABLE);

        UNIT_ASSERT_EXCEPTION(
            client->RunVanilla(TVanillaOperationSpec()
                .AddTask(TVanillaTask()
                    .Name("task")
                    .Job(new TFailingVanilla())
                    .JobCount(2))
                .StderrTablePath(stderrPath)
                .MaxFailedJobCount(5)),
            TOperationFailedError);

        UNIT_ASSERT_UNEQUAL(client->Get(stderrPath + "/@row_count"), 0);
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
            auto attrs = operation->GetAttributes(TGetOperationOptions().AttributeFilter(
                TOperationAttributeFilter().Add(EOperationAttribute::Type)));
            return *attrs.Type;
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
            UNIT_ASSERT_VALUES_EQUAL(getType(operation), EOperationType::Merge);
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
            UNIT_ASSERT_VALUES_EQUAL(getType(operation), EOperationType::Sort);
            UNIT_ASSERT_VALUES_EQUAL(getSortedBy(outputTable).Parts_, nonPrefixColumns.Parts_);
        }
    }

    void TestGetOperation_Completed(bool useClientGetOperation)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        auto beforeStart = TInstant::Now();
        auto op = client->Map(
            TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output"),
            new TIdMapper);
        auto afterFinish = TInstant::Now();

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

        auto inputTables = TNode().Add("//testing/input").AsList();
        UNIT_ASSERT(attrs.BriefSpec);
        UNIT_ASSERT(attrs.Spec);
        UNIT_ASSERT(attrs.FullSpec);
        UNIT_ASSERT_VALUES_EQUAL((*attrs.BriefSpec)["input_table_paths"].AsList(), inputTables);
        UNIT_ASSERT_VALUES_EQUAL((*attrs.Spec)["input_table_paths"].AsList(), inputTables);
        UNIT_ASSERT_VALUES_EQUAL((*attrs.FullSpec)["input_table_paths"].AsList(), inputTables);


        UNIT_ASSERT(attrs.Suspended);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.Suspended, false);

        UNIT_ASSERT(attrs.Result);
        UNIT_ASSERT(!attrs.Result->Error);

        UNIT_ASSERT(attrs.Progress);
        auto row_count = client->Get("//testing/input/@row_count").AsInt64();
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
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        auto op = client->Map(
            TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output")
                .MaxFailedJobCount(2),
            new TAlwaysFailingMapper,
            TOperationOptions()
                .Wait(false));

        op->Watch().Wait();

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

    Y_UNIT_TEST(ListOperations)
    {
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        TVector<IOperationPtr> operations;
        TVector<TInstant> beforeStartTimes;
        TVector<TInstant> afterFinishTimes;

        beforeStartTimes.push_back(TInstant::Now());
        auto mapOp = client->Map(
            TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output")
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
                .AddInput("//testing/input")
                .Output("//testing/input")
                .SortBy({"foo"})));
        afterFinishTimes.push_back(TInstant::Now());

        beforeStartTimes.push_back(TInstant::Now());
        operations.push_back(client->Reduce(
            TReduceOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output-with-great-name")
                .ReduceBy({"foo"}),
            new TIdReducer));
        afterFinishTimes.push_back(TInstant::Now());

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
            // TODO(levysotsky) Uncomment this check after YT-Arcadia sync.
            // UNIT_ASSERT_VALUES_EQUAL(*result.PoolCounts, (THashMap<TString, i64>{{"root", 3}}));

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
        auto client = CreateTestClient();

        CreateTableWithFooColumn(client, "//testing/input");

        auto op = client->Map(
            TMapOperationSpec()
                .AddInput<TNode>("//testing/input")
                .AddOutput<TNode>("//testing/output"),
            new TSleepingMapper(TDuration::Seconds(100)),
            TOperationOptions()
                .Spec(TNode()("weight", 5.0))
                .Wait(false));

        static auto getState = [](const IOperationPtr& op) {
            auto attrs = op->GetAttributes(TGetOperationOptions().AttributeFilter(
                TOperationAttributeFilter().Add(EOperationAttribute::State)));
            return *attrs.BriefState;
        };

        while (getState(op) != EOperationBriefState::InProgress) {
            Sleep(TDuration::MilliSeconds(100));
        }

        client->UpdateOperationParameters(op->GetId(),
            TNode()("scheduling_options_per_pool_tree",
                TNode()("default",
                    TNode()("weight", 10.0))));

        auto weightPath = "//sys/scheduler/orchid/scheduler/operations/" +
            GetGuidAsString(op->GetId()) +
            "/progress/scheduling_info_per_pool_tree/default/weight";
        UNIT_ASSERT_DOUBLES_EQUAL(client->Get(weightPath).AsDouble(), 10.0, 1e-9);
    }

    Y_UNIT_TEST(FormatHint)
    {
        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/input")
                .Schema(TTableSchema()
                    .Strict(true)
                    .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))
                    .AddColumn(TColumnSchema().Name("value").Type(VT_STRING))));

            writer->AddRow(TNode()("key", "foo")("value", TNode::CreateEntity()));
            writer->Finish();
        }
        const std::vector<TNode> expected = {TNode()("key", "foo")};
        auto readOutputAndRemove = [&] () {
            auto reader = client->CreateTableReader<TNode>("//testing/output");
            std::vector<TNode> result;
            for (; reader->IsValid(); reader->Next()) {
                result.push_back(reader->GetRow());
            }
            client->Remove("//testing/output");
            return result;
        };

        client->Map(
            TMapOperationSpec()
            .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output"),
            new TIdMapper);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->Reduce(
            TReduceOperationSpec()
            .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
            .ReduceBy("key")
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output"),
            new TIdReducer);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->MapReduce(
            TMapReduceOperationSpec()
            .ReduceBy("key")
            .MapperFormatHints(TUserJobFormatHints().InputFormatHints(TFormatHints().SkipNullValuesForTNode(true)))
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output"),
            new TIdMapper,
            new TIdReducer);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->MapReduce(
            TMapReduceOperationSpec()
            .ReduceBy("key")
            .ReducerFormatHints(TUserJobFormatHints().InputFormatHints(TFormatHints().SkipNullValuesForTNode(true)))
            .AddInput<TNode>("//testing/input")
            .AddOutput<TNode>("//testing/output"),
            new TIdMapper,
            new TIdReducer);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);
    }

    Y_UNIT_TEST(AttachOperation)
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
            new TSleepingMapper(TDuration::Seconds(100)),
            TOperationOptions().Wait(false));

        auto attached = client->AttachOperation(operation->GetId());

        attached->AbortOperation();

        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Aborted);
    }

    Y_UNIT_TEST(AttachInexistingOperation)
    {
        auto client = CreateTestClient();

        try {
            client->AttachOperation(GetGuid("1-2-3-4"));
            UNIT_FAIL("exception expected to be thrown");
        } catch (const TErrorResponse& e) {
            e.GetError().ContainsErrorCode(1915); // TODO: need named error code
        }
    }

    Y_UNIT_TEST(CrossTransactionMerge)
    {
        auto client = CreateTestClient();
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();

        {
            auto writer = tx1->CreateTableWriter<TNode>("//testing/input1");
            writer->AddRow(TNode()("row", "foo"));
            writer->Finish();
        }
        {
            auto writer = tx2->CreateTableWriter<TNode>("//testing/input2");
            writer->AddRow(TNode()("row", "bar"));
            writer->Finish();
        }
        client->Merge(
            TMergeOperationSpec()
            .AddInput(
                TRichYPath("//testing/input1")
                .TransactionId(tx1->GetId()))
            .AddInput(
                TRichYPath("//testing/input2")
                .TransactionId(tx2->GetId()))
            .Output("//testing/output"));
        tx1->Abort();
        tx2->Abort();

        TVector<TNode> expected = {
            TNode()("row", "foo"),
            TNode()("row", "bar"),
        };
        auto actual = ReadTable(client, "//testing/output");
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Completed);
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Failed);
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Completed);
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Completed);
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Failed);
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
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Failed);
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
