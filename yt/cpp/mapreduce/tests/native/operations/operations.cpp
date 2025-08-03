#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/tests/native/proto_lib/all_types.pb.h>
#include <yt/cpp/mapreduce/tests/native/proto_lib/all_types_proto3.pb.h>
#include <yt/cpp/mapreduce/tests/native/proto_lib/row.pb.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/common/debug_metrics.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <yt/cpp/mapreduce/library/lazy_sort/lazy_sort.h>

#include <yt/cpp/mapreduce/util/wait_for_tablets_state.h>

#include <library/cpp/digest/md5/md5.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/datetime/base.h>

#include <util/generic/maybe.h>
#include <util/generic/scope.h>
#include <util/generic/xrange.h>

#include <util/folder/path.h>

#include <util/string/split.h>
#include <util/string/hex.h>

#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/tempfile.h>
#include <util/generic/size_literals.h>

#include <util/thread/factory.h>

#include <utility>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

class TRangeBasedTIdMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (const auto& cursor : *reader) {
            writer->AddRow(cursor.GetRow());
        }
    }
};
REGISTER_MAPPER(TRangeBasedTIdMapper)

////////////////////////////////////////////////////////////////////////////////


class TMapperThatWritesToIncorrectTable
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader*, TWriter* writer) override
    {
        try {
            writer->AddRow(TNode(), 100500);
        } catch (...) {
        }
    }
};
REGISTER_MAPPER(TMapperThatWritesToIncorrectTable)

////////////////////////////////////////////////////////////////////////////////

class TMapperThatChecksFile
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TMapperThatChecksFile() = default;
    explicit TMapperThatChecksFile(TString file)
        : File_(std::move(file))
    { }

    void Do(TReader*, TWriter*) override
    {
        if (!TFsPath(File_).Exists()) {
            Cerr << "File `" << File_ << "' does not exist." << Endl;
            exit(1);
        }
    }

    Y_SAVELOAD_JOB(File_);

private:
    TString File_;
};
REGISTER_MAPPER(TMapperThatChecksFile)

////////////////////////////////////////////////////////////////////////////////

class TIdAndKvSwapMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
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
REGISTER_MAPPER(TIdAndKvSwapMapper)

////////////////////////////////////////////////////////////////////////////////

class TMapperThatReadsProtobufFile
    : public IMapper<TTableReader<TNode>, TTableWriter<TAllTypesMessage>>
{
public:
    TMapperThatReadsProtobufFile() = default;

    explicit TMapperThatReadsProtobufFile(TString file)
        : File_(std::move(file))
    { }

    void Do(TReader*, TWriter* writer) override
    {
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
REGISTER_MAPPER(TMapperThatReadsProtobufFile)

////////////////////////////////////////////////////////////////////////////////

class TProtobufMapper
    : public IMapper<TTableReader<TAllTypesMessage>, TTableWriter<TAllTypesMessage>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        TAllTypesMessage row;
        for (; reader->IsValid(); reader->Next()) {
            reader->MoveRow(&row);
            row.set_string_field(row.string_field() + " mapped");
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TProtobufMapper)

////////////////////////////////////////////////////////////////////////////////

class TProtobufMapperProto3
    : public IMapper<TTableReader<TAllTypesMessageProto3>, TTableWriter<TAllTypesMessageProto3>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        TAllTypesMessageProto3 row;
        for (; reader->IsValid(); reader->Next()) {
            reader->MoveRow(&row);
            row.set_string_field(row.string_field() + " mapped");
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TProtobufMapperProto3)

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class TWriter>
void ComplexTypesProtobufMapperDo(TReader* reader, TWriter* writer)
{
    for (; reader->IsValid(); reader->Next()) {
        if (reader->GetTableIndex() == 0) {
            auto row = reader->template MoveRow<TRowMixedSerializationOptions>();
            row.mutable_url_row_1()->set_host(row.url_row_1().host() + ".mapped");
            row.mutable_url_row_2()->set_host(row.url_row_2().host() + ".mapped");
            writer->AddRow(row, 0);
        } else {
            Y_ENSURE(reader->GetTableIndex() == 1);
            auto row = reader->template MoveRow<TRowSerializedRepeatedFields>();
            row.add_ints(40000);
            writer->AddRow(row, 1);
        }
    }
}

class TComplexTypesProtobufMapperMessage
    : public IMapper<TTableReader<Message>, TTableWriter<Message>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        ComplexTypesProtobufMapperDo(reader, writer);
    }
};
REGISTER_MAPPER(TComplexTypesProtobufMapperMessage)

class TComplexTypesProtobufMapperOneOf
    : public IMapper<
        TTableReader<TProtoOneOf<TRowMixedSerializationOptions, TRowSerializedRepeatedFields>>,
        TTableWriter<Message>
    >
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        ComplexTypesProtobufMapperDo(reader, writer);
    }
};
REGISTER_MAPPER(TComplexTypesProtobufMapperOneOf)

////////////////////////////////////////////////////////////////////////////////

class TProtobufMapperTypeOptions
    : public IMapper<TTableReader<Message>, TTableWriter<Message>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->MoveRow<TRowWithTypeOptions>();
            auto any = NodeFromYsonString(row.any_field());
            any["new"] = "delete";
            row.set_any_field(NodeToYsonString(any));
            row.add_repeated_enum_int_field(TRowWithTypeOptions::BLUE);
            auto otherColumns = NodeFromYsonString(row.other_columns_field());
            otherColumns["new_column"] = "BrandNew";
            row.set_other_columns_field(NodeToYsonString(otherColumns));
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TProtobufMapperTypeOptions)

////////////////////////////////////////////////////////////////////////////////

class TSplitGoodUrlMapper
    : public IMapper<TTableReader<TUrlRow>, TTableWriter<::google::protobuf::Message>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            const auto& urlRow = reader->GetRow();
            if (urlRow.http_code() == 200) {
                TGoodUrl goodUrl;
                goodUrl.set_url(urlRow.host() + urlRow.path());
                writer->AddRow(goodUrl, 1);
            }
            writer->AddRow(urlRow, 0);
        }
    }
};
REGISTER_MAPPER(TSplitGoodUrlMapper)

////////////////////////////////////////////////////////////////////////////////

class TCountHttpCodeTotalReducer
    : public IReducer<TTableReader<TUrlRow>, TTableWriter<THostRow>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        THostRow hostRow;
        i32 total = 0;
        for (; reader->IsValid(); reader->Next()) {
            const auto& urlRow = reader->GetRow();
            if (!hostRow.has_host()) {
                hostRow.set_host(urlRow.host());
            }
            total += urlRow.http_code();
        }
        hostRow.set_http_code_total(total);
        writer->AddRow(hostRow);
    }
};
REGISTER_REDUCER(TCountHttpCodeTotalReducer)

////////////////////////////////////////////////////////////////////////////////

class TJobBaseThatUsesEnv
{
public:
    TJobBaseThatUsesEnv() = default;

    explicit TJobBaseThatUsesEnv(TString envName)
        : EnvName_(std::move(envName))
    { }

    void Process(TTableReader<TNode>* reader, TTableWriter<TNode>* writer)
    {
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

class TMapperThatUsesEnv
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
    , public TJobBaseThatUsesEnv
{
public:
    TMapperThatUsesEnv() = default;
    explicit TMapperThatUsesEnv(TString envName)
        : TJobBaseThatUsesEnv(std::move(envName))
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        TJobBaseThatUsesEnv::Process(reader, writer);
    }

    Y_SAVELOAD_JOB(EnvName_);
};

REGISTER_MAPPER(TMapperThatUsesEnv)

////////////////////////////////////////////////////////////////////////////////

class TReducerThatUsesEnv
    : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
    , public TJobBaseThatUsesEnv
{
public:
    TReducerThatUsesEnv() = default;

    explicit TReducerThatUsesEnv(TString envName)
        : TJobBaseThatUsesEnv(std::move(envName))
    { }

    void Do(TReader* reader, TWriter* writer) override {
        TJobBaseThatUsesEnv::Process(reader, writer);
    }

    Y_SAVELOAD_JOB(EnvName_);
};

REGISTER_REDUCER(TReducerThatUsesEnv)

////////////////////////////////////////////////////////////////////////////////

class TMapperThatWritesCustomStatistics : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* /* reader */, TWriter* /* writer */) override
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
REGISTER_MAPPER(TMapperThatWritesCustomStatistics)

////////////////////////////////////////////////////////////////////////////////

class TVanillaAppendingToFile : public IVanillaJob<>
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
        file.Write(Message_.data(), Message_.size());
    }

    Y_SAVELOAD_JOB(FileName_, Message_);

private:
    TString FileName_;
    TString Message_;
};
REGISTER_VANILLA_JOB(TVanillaAppendingToFile)

////////////////////////////////////////////////////////////////////////////////

class TVanillaWithTableOutput
    : public IVanillaJob<TTableWriter<TNode>>
{
public:
    void Start(TWriter* writer) override
    {
        writer->AddRow(TNode()("first", 0)("second", 0), 0);
    }

    void Do(TWriter* writer) override
    {
        writer->AddRow(TNode()("first", 1)("second", 2), 0);
        writer->AddRow(TNode()("first", 3)("second", 4), 1);
    }

    void Finish(TWriter* writer) override
    {
        writer->AddRow(TNode()("first", 0)("second", 0), 1);
    }
};
REGISTER_VANILLA_JOB(TVanillaWithTableOutput)

////////////////////////////////////////////////////////////////////////////////

class TFailingVanilla : public IVanillaJob<>
{
public:
    void Do() override
    {
        Sleep(TDuration::Seconds(1));
        Cerr << "I'm writing to stderr, then gonna fail" << Endl;
        ::exit(1);
    }
};
REGISTER_VANILLA_JOB(TFailingVanilla)

////////////////////////////////////////////////////////////////////////////////

class TVanillaWithPorts : public IVanillaJob<>
{
public:
    TVanillaWithPorts() = default;
    TVanillaWithPorts(TStringBuf fileName, size_t portCount)
        : FileName_(fileName)
        , PortCount_(portCount)
    { }

    void Do() override
    {
        TOFStream stream(FileName_);
        for (const auto i: xrange(PortCount_)) {
            stream << GetEnv("YT_PORT_" + ToString(i)) << '\n';
        }
        stream.Flush();
    }

    Y_SAVELOAD_JOB(FileName_, PortCount_);

private:
    TString FileName_;
    size_t PortCount_;
};
REGISTER_VANILLA_JOB(TVanillaWithPorts)

////////////////////////////////////////////////////////////////////////////////

class TReducerThatSumsFirstThreeValues : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
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
REGISTER_REDUCER(TReducerThatSumsFirstThreeValues)

////////////////////////////////////////////////////////////////////////////////

template <class TInputRowTypeType>
class TMapperThatWritesRowsAndRanges : public ::IMapper<TTableReader<TInputRowTypeType>, TNodeWriter>
{
public:
    using TReader = TTableReader<TInputRowTypeType>;
    using TWriter = TNodeWriter;
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto row = TNode()
                ("row_id", reader->GetRowIndex())
                ("range_id", reader->GetRangeIndex());
            writer->AddRow(row);
        }
    }
};

REGISTER_MAPPER(TMapperThatWritesRowsAndRanges<TNode>)
REGISTER_MAPPER(TMapperThatWritesRowsAndRanges<TYaMRRow>)
REGISTER_MAPPER(TMapperThatWritesRowsAndRanges<TEmbeddedMessage>)

////////////////////////////////////////////////////////////////////////////////

class TMapperThatNumbersRows : public IMapper<TNodeReader, TNodeWriter>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            row["INDEX"] = reader->GetRowIndex();
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TMapperThatNumbersRows)

////////////////////////////////////////////////////////////////////////////////

class TReducerThatCountsOutputTables : public IReducer<TNodeReader, TNodeWriter>
{
public:
    TReducerThatCountsOutputTables() = default;

    void Do(TReader*, TWriter* writer) override
    {
        writer->AddRow(TNode()("result", GetOutputTableCount()), 0);
    }
};
REGISTER_REDUCER(TReducerThatCountsOutputTables)

////////////////////////////////////////////////////////////////////////////////

class TIdTRowVer2Mapper : public IMapper<TTableReader<Message>, TTableWriter<TRowVer2>>
{
    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            writer->AddRow(cursor.GetRow<TRowVer2>());
        }
    }
};

REGISTER_MAPPER(TIdTRowVer2Mapper)

////////////////////////////////////////////////////////////////////////////////

class TMapperForOrderedDynamicTables : public IMapper<TNodeReader, TNodeWriter>
{
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            row["range_index"] = reader->GetRangeIndex();
            row["tablet_index"] = reader->GetTabletIndex();
            row["row_index"] = reader->GetRowIndex();
            writer->AddRow(row);
        }
    }
};

REGISTER_MAPPER(TMapperForOrderedDynamicTables)

////////////////////////////////////////////////////////////////////////////////

void TestRenameColumns(ENodeReaderFormat nodeReaderFormat)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = nodeReaderFormat;

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
                .Schema(TTableSchema()
                    .AddColumn(TColumnSchema().Name("OldKey").Type(VT_STRING))
                    .AddColumn(TColumnSchema().Name("Value").Type(VT_STRING))
                    .Strict(true)));
        writer->AddRow(TNode()("OldKey", "key")("Value", "value"));
        writer->Finish();
    }

    THashMap<TString, TString> columnMapping;
    columnMapping["OldKey"] = "NewKey";

    client->Map(
        TMapOperationSpec()
        .AddInput<TNode>(
            TRichYPath(workingDir + "/input")
                .RenameColumns(columnMapping))
        .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper);

    auto actual = ReadTable(client, workingDir + "/output");
    const auto expected = TVector({
        TNode()("NewKey", "key")("Value", "value"),
    });
    EXPECT_EQ(actual, expected);
}

TEST(Operations, RenameColumns_Yson)
{
    TestRenameColumns(ENodeReaderFormat::Yson);
}

TEST(Operations, RenameColumns_Skiff)
{
    TestRenameColumns(ENodeReaderFormat::Skiff);
}

TEST(Operations, IncorrectTableId)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    client->Map(
        TMapOperationSpec()
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output")
        .MaxFailedJobCount(1),
        new TMapperThatWritesToIncorrectTable);
}

TEST(Operations, EnableKeyGuarantee)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
                .Schema(TTableSchema()
                    .Strict(true)
                    .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))));
        writer->AddRow(TNode()("key", "foo"));
        writer->Finish();
    }

    auto op = client->Reduce(
        TReduceOperationSpec()
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output")
        .ReduceBy("key")
        .EnableKeyGuarantee(false),
        new TIdReducer);
    auto spec = client->GetOperation(op->GetId()).Spec;
    EXPECT_EQ((*spec)["enable_key_guarantee"].AsBool(), false);
}

TEST(Operations, OrderedMapReduce)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
                .Schema(TTableSchema()
                    .Strict(true)
                    .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))));
        writer->AddRow(TNode()("key", "foo"));
        writer->Finish();
    }

    auto op = client->MapReduce(
        TMapReduceOperationSpec()
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output")
        .ReduceBy("key")
        .Ordered(true),
        new TIdMapper,
        new TIdReducer);
    auto spec = client->GetOperation(op->GetId()).Spec;
    EXPECT_EQ((*spec)["ordered"].AsBool(), true);
}

TEST(Operations, CustomTitle)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    {
        auto newTitle = "MyCustomTitle";

        auto op = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .Title(newTitle),
            new TIdMapper);
        auto spec = client->GetOperation(op->GetId()).Spec;
        EXPECT_EQ((*spec)["title"].AsString(), newTitle);
    }

    {
        auto op = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdMapper);
        auto spec = client->GetOperation(op->GetId()).Spec;
        EXPECT_TRUE((*spec)["title"].AsString().Contains("TIdMapper"));
    }
}

TEST(Operations, RangeBasedReader)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TVector<TNode> data = {
        TNode()("foo", "bar"),
        TNode()("foo", "baz"),
        TNode()("foo", "qux")
    };
    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        for (const auto& row : data) {
            writer->AddRow(row);
        }
        writer->Finish();
    }

    auto outputTable = workingDir + "/output";
    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(outputTable),
        new TRangeBasedTIdMapper);

    TVector<TNode> result = ReadTable(client, outputTable);
    EXPECT_EQ(data, result);
}

TEST(Operations, RangeBasedReaderEmptyTable)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto outputTable = workingDir + "/output";
    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->Finish();
    }

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(outputTable),
        new TRangeBasedTIdMapper);

    TVector<TNode> result = ReadTable(client, outputTable);
    EXPECT_TRUE(result.empty());
}

TEST(Operations, MaxFailedJobCount)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    for (const auto maxFailedJobCount : {1, 7}) {
        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MaxFailedJobCount(maxFailedJobCount),
            new TAlwaysFailingMapper,
            TOperationOptions()
                .Wait(false));
        auto future = operation->Watch();
        future.Wait();
        EXPECT_THROW(future.GetValue(), TOperationFailedError);

        WaitOperationHasBriefProgress(operation);

        const auto& briefProgress = operation->GetBriefProgress();
        EXPECT_TRUE(briefProgress);
        EXPECT_EQ(static_cast<int>(briefProgress->Failed), maxFailedJobCount);
    }
}

TEST(Operations, FailOnJobRestart)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    TOperationId operationId;
    try {
        client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .FailOnJobRestart(true)
            .MaxFailedJobCount(3),
            new TAlwaysFailingMapper);
        FAIL() << "Operation expected to fail";
    } catch (const TOperationFailedError& e) {
        operationId = e.GetOperationId();
    }

    const auto& briefProgress = client->GetOperation(operationId).BriefProgress;
    EXPECT_TRUE(briefProgress);
    EXPECT_EQ(static_cast<int>(briefProgress->Failed), 1);
}

TEST(Operations, StderrTablePath)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    TStringBuf expectedStderr = "PYSHCH";
    client->Map(
        TMapOperationSpec()
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output")
        .StderrTablePath(workingDir + "/stderr"),
        new TMapperThatWritesStderr(expectedStderr));

    auto result = ReadTable(client, workingDir + "/stderr");
    EXPECT_EQ(std::ssize(result), 1);
    EXPECT_TRUE(result[0]["data"].AsString().Contains(expectedStderr));
}

TEST(Operations, CreateDebugOutputTables)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    TStringBuf expectedStderr = "PYSHCH";

    // stderr table does not exist => should fail
    EXPECT_THROW(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output")
                .StderrTablePath(workingDir + "/stderr"),
            new TMapperThatWritesStderr(expectedStderr),
            TOperationOptions()
                .CreateDebugOutputTables(false)),
        TOperationFailedError);

    client->Create(workingDir + "/stderr", NT_TABLE);

    // stderr table exists => should pass
    EXPECT_NO_THROW(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output")
                .StderrTablePath(workingDir + "/stderr"),
            new TMapperThatWritesStderr(expectedStderr),
            TOperationOptions()
                .CreateDebugOutputTables(false)));
}

TEST(Operations, CreateOutputTables)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    TStringBuf expectedStderr = "PYSHCH";

    // Output table does not exist => operation should fail.
    EXPECT_THROW(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output")
                .StderrTablePath(workingDir + "/stderr"),
            new TMapperThatWritesStderr(expectedStderr),
            TOperationOptions()
                .CreateOutputTables(false)),
        TOperationFailedError);

    client->Create(workingDir + "/output", NT_TABLE);

    // Output table exists => should complete ok.
    EXPECT_NO_THROW(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output")
                .StderrTablePath(workingDir + "/stderr"),
            new TMapperThatWritesStderr(expectedStderr),
            TOperationOptions()
                .CreateOutputTables(false)));

    // Inputs not checked => we get TApiUsageError.
    EXPECT_THROW(
        client->Sort(
            TSortOperationSpec()
                .AddInput(workingDir + "/nonexistent-input")
                .Output(workingDir + "/nonexistent-input")),
        TApiUsageError);

    // Inputs are not checked => we get an error response from the server.
    EXPECT_THROW(
        client->Sort(
            TSortOperationSpec()
                .AddInput(workingDir + "/nonexistent-input")
                .Output(workingDir + "/nonexistent-input"),
            TOperationOptions()
                .CreateOutputTables(false)),
        TOperationFailedError);
}

TEST(Operations, TestFetchTable)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    // Expect operation to complete successfully
    client->Map(
        TMapOperationSpec()
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output")
        .MapperSpec(TUserJobSpec().AddFile(TRichYPath(workingDir + "/input").Format("yson"))),
        new TMapperThatChecksFile("input"));
}

TEST(Operations, TestFetchTableRange)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    // Expect operation to complete successfully
    client->Map(
        TMapOperationSpec()
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output")
        .MapperSpec(TUserJobSpec().AddFile(TRichYPath(workingDir + "/input[#0]").Format("yson"))),
        new TMapperThatChecksFile("input"));
}

TEST(Operations, TestReadProtobufFileInJob)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TAllTypesMessage message;
    message.set_fixed32_field(2134242);
    message.set_sfixed32_field(422142);
    message.set_bool_field(true);
    message.set_string_field("42");
    message.set_bytes_field("36 popugayev");
    message.set_enum_field(EEnum::One);
    message.mutable_message_field()->set_key("key");
    message.mutable_message_field()->set_value("value");

    {
        auto writer = client->CreateTableWriter<TAllTypesMessage>(workingDir + "/input");
        writer->AddRow(message);
        writer->Finish();
    }

    auto format = TFormat::Protobuf<TAllTypesMessage>();

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TAllTypesMessage>(workingDir + "/output")
            .MapperSpec(TUserJobSpec().AddFile(TRichYPath(workingDir + "/input").Format(format.Config))),
        new TMapperThatReadsProtobufFile("input"));

    {
        auto reader = client->CreateTableReader<TAllTypesMessage>(workingDir + "/output");
        EXPECT_TRUE(reader->IsValid());
        const auto& row = reader->GetRow();
        EXPECT_EQ(message.fixed32_field(), row.fixed32_field());
        EXPECT_EQ(message.sfixed32_field(), row.sfixed32_field());
        EXPECT_EQ(message.bool_field(), row.bool_field());
        EXPECT_EQ(message.string_field(), row.string_field());
        EXPECT_EQ(message.bytes_field(), row.bytes_field());
        EXPECT_EQ(message.enum_field(), row.enum_field());
        EXPECT_EQ(message.message_field().key(), row.message_field().key());
        EXPECT_EQ(message.message_field().value(), row.message_field().value());
    }
}

TEST(Operations, TestGetOperationStatus_Completed)
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

    while (operation->GetBriefState() == EOperationBriefState::InProgress) {
        Sleep(TDuration::MilliSeconds(100));
    }
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_TRUE(operation->GetError().Empty());

    EmulateOperationArchivation(client, operation->GetId());
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Completed);
    EXPECT_TRUE(operation->GetError().Empty());
}

TEST(Operations, TestGetOperationStatus_Failed)
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

    while (operation->GetBriefState() == EOperationBriefState::InProgress) {
        Sleep(TDuration::MilliSeconds(100));
    }
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Failed);
    EXPECT_TRUE(operation->GetError().Defined());

    EmulateOperationArchivation(client, operation->GetId());
    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Failed);
    EXPECT_TRUE(operation->GetError().Defined());
}

TEST(Operations, TestGetOperationStatistics)
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
    auto jobStatistics = operation->GetJobStatistics();
    EXPECT_TRUE(jobStatistics.GetStatistics("time/total").Max().Defined());
}

TEST(Operations, TestCustomStatistics)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }
    auto operation = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TMapperThatWritesCustomStatistics());

    auto jobStatistics = operation->GetJobStatistics();

    auto first = jobStatistics.GetCustomStatistics("some/path/to/stat").Max();
    EXPECT_TRUE(*first == std::numeric_limits<i64>::min());

    auto second = jobStatistics.GetCustomStatistics("second/second-and-half").Max();
    EXPECT_TRUE(*second == -142);

    auto another = jobStatistics.GetCustomStatistics("another/path/to/stat\\/with\\/escaping").Max();
    EXPECT_TRUE(*another == 43);

    auto unescaped = jobStatistics.GetCustomStatistics("ambiguous/path").Max();
    EXPECT_TRUE(*unescaped == 7331);

    auto escaped = jobStatistics.GetCustomStatistics("ambiguous\\/path").Max();
    EXPECT_TRUE(*escaped == 1337);
}

void MapWithProtobuf(bool useDeprecatedAddInput, bool useClientProtobuf, bool enableTabletIndex = false)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseClientProtobuf = useClientProtobuf;

    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("string_field", "raz"));
        writer->AddRow(TNode()("string_field", "dva"));
        writer->AddRow(TNode()("string_field", "tri"));
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

    auto specNode = TNode();
    specNode["job_io"]["control_attributes"]["enable_tablet_index"] = enableTabletIndex;
    client->Map(spec, new TProtobufMapper, TOperationOptions().Spec(specNode));

    TVector<TNode> expected = {
        TNode()("string_field", "raz mapped"),
        TNode()("string_field", "dva mapped"),
        TNode()("string_field", "tri mapped"),
    };
    auto actual = ReadTable(client, outputTable.Path_);
    EXPECT_EQ(expected, actual);
}

TEST(Operations, ProtobufMapProto3)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("string_field", "raz"));
        writer->AddRow(TNode()("string_field", "dva"));
        writer->AddRow(TNode()("string_field", "tri"));
        writer->Finish();
    }
    auto spec = TMapOperationSpec()
        .AddInput<TAllTypesMessageProto3>(inputTable)
        .AddOutput<TAllTypesMessageProto3>(outputTable);

    client->Map(spec, new TProtobufMapperProto3);

    TVector<TNode> expected = {
        TNode()("string_field", "raz mapped"),
        TNode()("string_field", "dva mapped"),
        TNode()("string_field", "tri mapped"),
    };
    auto actual = ReadTable(client, outputTable.Path_);
    EXPECT_EQ(expected, actual);
}

TEST(Operations, ProtobufMap_NativeProtobuf)
{
    MapWithProtobuf(false, false);
}

TEST(Operations, ProtobufMap_ClientProtobuf)
{
    MapWithProtobuf(false, true);
}

TEST(Operations, ProtobufMap_Input_VerySlow_Deprecated_NativeProtobuf)
{
    MapWithProtobuf(true, false);
}

TEST(Operations, ProtobufMap_Input_VerySlow_Deprecated_ClientProtobuf)
{
    MapWithProtobuf(true, true);
}

TEST(Operations, ProtobufMap_EnableTabletIndex)
{
    MapWithProtobuf(false, false, /* enableTabletIndex */ true);
}

void TestProtobufMap_ComplexTypes(bool useOneOfMapper)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto urlRowRawTypeV3 = TNode()
        ("type_name", "struct")
        ("members", TNode()
            .Add(TNode()("name", "Host")("type", "string"))
            .Add(TNode()("name", "Path")("type", "string"))
            .Add(TNode()("name", "HttpCode")("type", "int32")));

    auto schema1 = TTableSchema()
        .AddColumn(TColumnSchema().Name("url_row_1").RawTypeV3(urlRowRawTypeV3))
        .AddColumn(TColumnSchema().Name("url_row_2").Type(VT_STRING));

    auto schema2 = TTableSchema()
        .AddColumn(TColumnSchema()
            .Name("ints")
            .RawTypeV3(TNode()
                ("type_name", "list")
                ("item", "int64")))
        .AddColumn(TColumnSchema()
            .Name("url_rows")
            .RawTypeV3(TNode()
                ("type_name", "list")
                ("item", urlRowRawTypeV3)));

    auto inputTable1 = TRichYPath(workingDir + "/input_1").Schema(schema1);
    auto inputTable2 = TRichYPath(workingDir + "/input_2").Schema(schema2);
    auto outputTable1 = TRichYPath(workingDir + "/output_1").Schema(schema1);
    auto outputTable2 = TRichYPath(workingDir + "/output_2").Schema(schema2);

    {
        // TRowMixedSerializationOptions.
        // url_row_2 has the same value as url_row_1.
        auto writer = client->CreateTableWriter<TNode>(inputTable1);
        writer->AddRow(TNode()
            ("url_row_1", TNode()("Host", "ya.ru")("Path", "/mail")("HttpCode", 404))
            ("url_row_2",
                    "\x0A" "\x05" "\x79\x61\x2E\x72\x75"
                    "\x12" "\x05" "\x2F\x6D\x61\x69\x6C"
                    "\x18" "\xA8\x06"));
        writer->AddRow(TNode()
            ("url_row_1", TNode()("Host", "ya.ru")("Path", "/maps")("HttpCode", 300))
            ("url_row_2",
                "\x0A" "\x05" "\x79\x61\x2E\x72\x75"
                "\x12" "\x05" "\x2F\x6D\x61\x70\x73"
                "\x18" "\xD8\x04"));
        writer->Finish();
    }

    {
        // TRowSerializedRepeatedFields.
        auto writer = client->CreateTableWriter<TNode>(inputTable2);
        writer->AddRow(TNode()
            ("ints", TNode().Add(-1).Add(-2))
            ("url_rows", TNode()
                .Add(TNode()("Host", "yandex.ru")("Path", "/mail")("HttpCode", 200))
                .Add(TNode()("Host", "google.com")("Path", "/mail")("HttpCode", 404))));
        writer->AddRow(TNode()
            ("ints", TNode().Add(1).Add(2))
            ("url_rows", TNode()
                .Add(TNode()("Host", "yandex.ru")("Path", "/maps")("HttpCode", 200))
                .Add(TNode()("Host", "google.com")("Path", "/maps")("HttpCode", 404))));
        writer->Finish();
    }

    ::TIntrusivePtr<IMapperBase> mapper;
    if (useOneOfMapper) {
        mapper = new TComplexTypesProtobufMapperOneOf;
    } else {
        mapper = new TComplexTypesProtobufMapperMessage;
    }

    client->Map(
        TMapOperationSpec()
            .AddInput<TRowMixedSerializationOptions>(inputTable1)
            .AddInput<TRowSerializedRepeatedFields>(inputTable2)
            .AddOutput<TRowMixedSerializationOptions>(outputTable1)
            .AddOutput<TRowSerializedRepeatedFields>(outputTable2),
        mapper);

    TVector<TNode> expectedContent1 = {
        TNode()
            ("url_row_1", TNode()("Host", "ya.ru.mapped")("Path", "/mail")("HttpCode", 404))
            ("url_row_2",
                "\x0A" "\x0C" "\x79\x61\x2E\x72\x75\x2E\x6D\x61\x70\x70\x65\x64"
                "\x12" "\x05" "\x2F\x6D\x61\x69\x6C"
                "\x18" "\xA8\x06"),
        TNode()
            ("url_row_1", TNode()("Host", "ya.ru.mapped")("Path", "/maps")("HttpCode", 300))
            ("url_row_2",
                "\x0A" "\x0C" "\x79\x61\x2E\x72\x75\x2E\x6D\x61\x70\x70\x65\x64"
                "\x12" "\x05" "\x2F\x6D\x61\x70\x73"
                "\x18" "\xD8\x04"),
    };

    TVector<TNode> expectedContent2 = {
        TNode()
            ("ints", TNode().Add(-1).Add(-2).Add(40000))
            ("url_rows", TNode()
                .Add(TNode()("Host", "yandex.ru")("Path", "/mail")("HttpCode", 200))
                .Add(TNode()("Host", "google.com")("Path", "/mail")("HttpCode", 404))),
        TNode()
            ("ints", TNode().Add(1).Add(2).Add(40000))
            ("url_rows", TNode()
                .Add(TNode()("Host", "yandex.ru")("Path", "/maps")("HttpCode", 200))
                .Add(TNode()("Host", "google.com")("Path", "/maps")("HttpCode", 404)))
    };

    auto actualContent1 = ReadTable(client, outputTable1.Path_);
    auto actualContent2 = ReadTable(client, outputTable2.Path_);

    EXPECT_EQ(expectedContent1, actualContent1);
    EXPECT_EQ(expectedContent2, actualContent2);
}

TEST(Operations, ProtobufMap_ComplexTypes_Message)
{
    TestProtobufMap_ComplexTypes(/* useOneOf */ false);
}

TEST(Operations, ProtobufMap_ComplexTypes_OneOf)
{
    TestProtobufMap_ComplexTypes(/* useOneOf */ true);
}

TEST(Operations, ProtobufMap_TypeOptions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto schema = TTableSchema()
        .Strict(false)
        .AddColumn(TColumnSchema()
            .Name("color_int_field").Type(EValueType::VT_INT64))
        .AddColumn(TColumnSchema()
            .Name("color_string_field").Type(EValueType::VT_STRING))
        .AddColumn(TColumnSchema()
            .Name("any_field").Type(EValueType::VT_ANY))
        .AddColumn(TColumnSchema()
            .Name("embedded_field").RawTypeV3(TNode()
                ("type_name", "optional")
                ("item", TNode()
                    ("type_name", "struct")
                    ("members", TNode()
                        .Add(TNode()
                            ("name", "color_int_field")
                            ("type", "int64"))
                        .Add(TNode()
                            ("name", "color_string_field")
                            ("type", "string"))
                        .Add(TNode()
                            ("name", "any_field")
                            ("type", TNode()
                                ("type_name", "optional")
                                ("item", "yson")))))))
        .AddColumn(TColumnSchema()
            .Name("repeated_enum_int_field").RawTypeV3(TNode()
                ("type_name", "list")
                ("item", "int64")))
        .AddColumn(TColumnSchema()
            .Name("unknown_schematized_column").Type(EValueType::VT_BOOLEAN));

    auto inputTable = TRichYPath(workingDir + "/input").Schema(schema);
    auto outputTable = TRichYPath(workingDir + "/output").Schema(schema);

    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()
            ("color_int_field", -1)
            ("color_string_field", "BLUE")
            ("any_field", TNode()("x", TNode()("y", 12)))
            ("unknown_schematized_column", true)
            ("unknown_unschematized_column", 1234)
            ("embedded_field", TNode()
                ("color_int_field", 0)
                ("color_string_field", "RED")
                ("any_field", TNode()("key", "value")))
            ("repeated_enum_int_field", TNode().Add(0).Add(1).Add(-1)));
        writer->AddRow(TNode()
            ("color_int_field", 0)
            ("color_string_field", "RED")
            ("any_field", TNode()("z", 0))
            ("unknown_schematized_column", false)
            ("unknown_unschematized_column", "some-string")
            ("embedded_field", TNode()
                ("color_int_field", -1)
                ("color_string_field", "WHITE")
                ("any_field", "hooray"))
            ("repeated_enum_int_field", TNode().Add(1)));
        writer->Finish();
    }

    client->Map(
        TMapOperationSpec()
            .AddInput<TRowWithTypeOptions>(inputTable)
            .AddOutput<TRowWithTypeOptions>(outputTable),
        new TProtobufMapperTypeOptions);

    auto actualRows = ReadTable(client, outputTable.Path_);
    EXPECT_EQ(std::ssize(actualRows), 2);
    {
        const auto& row = actualRows[0];
        EXPECT_EQ(row["color_int_field"], -1);
        EXPECT_EQ(row["color_string_field"], "BLUE");
        EXPECT_EQ(
            row["any_field"],
            TNode()
                ("x", TNode()("y", 12))
                ("new", "delete"));
        EXPECT_EQ(row["unknown_schematized_column"], true);
        EXPECT_EQ(row["unknown_unschematized_column"], 1234);
        EXPECT_EQ(
            row["embedded_field"],
            TNode()
                ("color_int_field", 0)
                ("color_string_field", "RED")
                ("any_field", TNode()("key", "value")));
        EXPECT_EQ(row["repeated_enum_int_field"], TNode().Add(0).Add(1).Add(-1).Add(1));
        EXPECT_EQ(row["new_column"], "BrandNew");
    }
    {
        const auto& row = actualRows[1];
        EXPECT_EQ(row["color_int_field"], 0);
        EXPECT_EQ(row["color_string_field"], "RED");
        EXPECT_EQ(
            row["any_field"],
            TNode()
                ("z", 0)
                ("new", "delete"));
        EXPECT_EQ(row["unknown_schematized_column"], false);
        EXPECT_EQ(row["unknown_unschematized_column"], "some-string");
        EXPECT_EQ(row["embedded_field"], TNode()
            ("color_int_field", -1)
            ("color_string_field", "WHITE")
            ("any_field", "hooray"));
        EXPECT_EQ(row["repeated_enum_int_field"], TNode().Add(1).Add(1));
    }
}

TEST(Operations, JobPrefix)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
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
        EXPECT_EQ(reader->GetRow()["TEST_ENV"], "");
    }

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputTable)
            .AddOutput<TNode>(outputTable),
        new TMapperThatUsesEnv("TEST_ENV"),
        TOperationOptions().JobCommandPrefix("TEST_ENV=common "));
    {
        auto reader = client->CreateTableReader<TNode>(outputTable);
        EXPECT_EQ(reader->GetRow()["TEST_ENV"], "common");
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
        EXPECT_EQ(reader->GetRow()["TEST_ENV"], "mapper");
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
        EXPECT_EQ(reader->GetRow()["TEST_ENV"], "mapper");
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
        EXPECT_EQ(reader->GetRow()["TEST_ENV"], "mapperreducer");
    }
}

TEST(Operations, JobEnvironment)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("input", "dummy"));
        writer->Finish();
    }

    client->Map(
        TMapOperationSpec()
            .MapperSpec(TUserJobSpec().AddEnvironment("TEST_ENV", "foo bar baz"))
            .AddInput<TNode>(inputTable)
            .AddOutput<TNode>(outputTable),
        new TMapperThatUsesEnv("TEST_ENV"),
        TOperationOptions());
    {
        auto reader = client->CreateTableReader<TNode>(outputTable);
        EXPECT_EQ(reader->GetRow()["TEST_ENV"], "foo bar baz");
    }
}

TEST(Operations, MapReduceMapOutput)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("key", "foo")("value", "bar"));
        writer->Finish();
    }

    client->MapReduce(
        TMapReduceOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddMapOutput<TNode>(workingDir + "/map_output")
            .AddOutput<TNode>(workingDir + "/output")
            .ReduceBy({"key"}),
        new TIdAndKvSwapMapper,
        new TIdReducer);

    EXPECT_EQ(
        ReadTable(client, workingDir + "/output"),
        TVector<TNode>{TNode()("key", "foo")("value", "bar")});

    EXPECT_EQ(
        ReadTable(client, workingDir + "/map_output"),
        TVector<TNode>{TNode()("key", "bar")("value", "foo")});
}

template<typename TUrlRow, typename TGoodUrl, typename THostRow, class TMapper, class TReducer>
void TestMapReduceMapOutput()
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto writer = client->CreateTableWriter<TUrlRow>(workingDir + "/input");
        TUrlRow row;
        row.set_host("http://example.com");
        row.set_path("/index.php");
        row.set_http_code(200);
        writer->AddRow(row);
        writer->Finish();
    }

    client->MapReduce(
        TMapReduceOperationSpec()
            .template AddInput<TUrlRow>(workingDir + "/input")
            .template HintMapOutput<TUrlRow>()
            .template AddMapOutput<TGoodUrl>(workingDir + "/map_output")
            .template AddOutput<THostRow>(workingDir + "/output")
            .ReduceBy({"Host"}),
        new TMapper,
        new TReducer);

    EXPECT_EQ(
        ReadTable(client, workingDir + "/output"),
        TVector<TNode>{TNode()("Host", "http://example.com")("HttpCodeTotal", 200)});

    EXPECT_EQ(
        ReadTable(client, workingDir + "/map_output"),
        TVector<TNode>{TNode()("Url", "http://example.com/index.php")});
}

TEST(Operations, MapReduceMapOutputProtobuf)
{
    TestMapReduceMapOutput<
        TUrlRow,
        TGoodUrl,
        THostRow,
        TSplitGoodUrlMapper,
        TCountHttpCodeTotalReducer>();
}

TEST(Operations, AddLocalFile)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
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
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .MapperSpec(TUserJobSpec().AddLocalFile("localPath", TAddLocalFileOptions().PathInJob("path/in/job"))),
        new TMapperThatChecksFile("path/in/job"));
}

TEST(Operations, TestFailWithNoInput)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        EXPECT_THROW(client->Map(
            TMapOperationSpec()
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdMapper), TApiUsageError);
    }
}

TEST(Operations, MaxOperationCountExceeded)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->StartOperationRetryCount = 3;
    TConfig::Get()->StartOperationRetryInterval = TDuration::MilliSeconds(0);

    size_t maxOperationCount = 1;
    client->Create(
        "",
        NT_SCHEDULER_POOL,
        TCreateOptions().Attributes(NYT::TNode()
            ("name", "research")
            ("pool_tree", "default")));
    client->Create(
        "",
        NT_SCHEDULER_POOL,
        TCreateOptions().Attributes(NYT::TNode()
            ("name", "testing")
            ("pool_tree", "default")
            ("parent_name", "research")));
    client->Set("//sys/pools/research/testing/@max_operation_count", maxOperationCount);

    CreateTableWithFooColumn(client, workingDir + "/input");

    TVector<IOperationPtr> operations;

    Y_DEFER {
        for (auto& operation : operations) {
            operation->AbortOperation();
        }
    };

    try {
        for (size_t i = 0; i < maxOperationCount + 1; ++i) {
            operations.push_back(client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(workingDir + "/input")
                    .AddOutput<TNode>(workingDir + "/output_" + ToString(i)),
                new TSleepingMapper(TDuration::Seconds(3600)),
                TOperationOptions()
                    .Spec(TNode()("pool", "testing"))
                    .Wait(false)));
        }
        FAIL() << "Too many Maps must have been failed";
    } catch (const TErrorResponse& error) {
        // It's OK
    }
}

TEST(Operations, NetworkProblems)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->StartOperationRetryCount = 3;
    TConfig::Get()->StartOperationRetryInterval = TDuration::MilliSeconds(0);

    CreateTableWithFooColumn(client, workingDir + "/input");

    try {
        auto outage = TAbortableHttpResponse::StartOutage("/start_op");
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output_1"),
            new TIdMapper());
        FAIL() << "Start operation must have been failed";
    } catch (const TAbortedForTestPurpose&) {
        // It's OK
    }
    {
        auto outage = TAbortableHttpResponse::StartOutage("/start_op", TConfig::Get()->StartOperationRetryCount - 1);
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output_2"),
            new TIdMapper());
    }
}

void TestJobNodeReader(ENodeReaderFormat nodeReaderFormat, bool strictSchema)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = nodeReaderFormat;

    TString inputPath = workingDir + "/input";
    TString outputPath = workingDir + "/input";
    Y_DEFER {
        client->Remove(inputPath, TRemoveOptions().Force(true));
    };

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

    auto result = ReadTable(client, outputPath);
    EXPECT_EQ(result, TVector<TNode>{row});
}

TEST(Operations, JobNodeReader_Skiff_Strict)
{
    TestJobNodeReader(ENodeReaderFormat::Skiff, true);
}
TEST(Operations, JobNodeReader_Skiff_NonStrict)
{
    EXPECT_THROW(TestJobNodeReader(ENodeReaderFormat::Skiff, false), yexception);
}
TEST(Operations, JobNodeReader_Auto_Strict)
{
    TestJobNodeReader(ENodeReaderFormat::Auto, true);
}
TEST(Operations, JobNodeReader_Auto_NonStrict)
{
    TestJobNodeReader(ENodeReaderFormat::Auto, false);
}
TEST(Operations, JobNodeReader_Yson_Strict)
{
    TestJobNodeReader(ENodeReaderFormat::Yson, true);
}
TEST(Operations, JobNodeReader_Yson_NonStrict)
{
    TestJobNodeReader(ENodeReaderFormat::Yson, false);
}
TEST(Operations, JobNodeReader_Skiff_ComplexTypes)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto inTablePath = workingDir + "/in-table";
    auto outTablePath = workingDir + "/out-table";

    TTableSchema schema;
    schema.AddColumn("value", NTi::List(NTi::Int64()));

    client->Create(inTablePath, NT_TABLE, TCreateOptions().Attributes(TNode()("schema", schema.ToNode())));

    const auto expected = TVector<TNode>{
        TNode()("value", TNode::CreateList({TNode(1), TNode(2), TNode(3)})),
    };
    {
        auto writer = client->CreateTableWriter<TNode>(inTablePath);
        for (const auto& row : expected) {
            writer->AddRow(row);
        }
        writer->Finish();
    }
    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;
    client->Map(new TIdMapper(), inTablePath, outTablePath);
    TVector<TNode> actual = ReadTable(client, outTablePath);
    EXPECT_EQ(expected, actual);
}

TEST(Operations, TestSkiffAllTypes)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto row = TNode()
            ("int8", 1)
            ("int16", 300)
            ("int32", 1000000)
            ("int64", -5000000000000ll)

            ("uint8", 1ull)
            ("uint16", 300ull)
            ("uint32", 1000000ull)
            ("uint64", 5000000000000ull)

            ("float", 2.71)
            ("double", 3.14)

            ("bool", true)

            ("string", "foo")
            ("utf8", "bar")
            ("json", "[]")

            ("date", 3)
            ("datetime", 85)
            ("timestamp", 100400)
            ("interval", -28)

            ("null", TNode::CreateEntity())
            ("void", TNode::CreateEntity());

        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
            .Schema(TTableSchema()
                .Strict(true)
                .AddColumn(TColumnSchema().Name("int8").Type(NTi::Int8()))
                .AddColumn(TColumnSchema().Name("int16").Type(NTi::Int16()))
                .AddColumn(TColumnSchema().Name("int32").Type(NTi::Int32()))
                .AddColumn(TColumnSchema().Name("int64").Type(NTi::Int64()))

                .AddColumn(TColumnSchema().Name("uint8").Type(NTi::Uint8()))
                .AddColumn(TColumnSchema().Name("uint16").Type(NTi::Uint16()))
                .AddColumn(TColumnSchema().Name("uint32").Type(NTi::Uint32()))
                .AddColumn(TColumnSchema().Name("uint64").Type(NTi::Uint64()))

                .AddColumn(TColumnSchema().Name("float").Type(NTi::Float()))
                .AddColumn(TColumnSchema().Name("double").Type(NTi::Double()))

                .AddColumn(TColumnSchema().Name("bool").Type(NTi::Bool()))

                .AddColumn(TColumnSchema().Name("string").Type(NTi::String()))
                .AddColumn(TColumnSchema().Name("utf8").Type(NTi::Utf8()))
                .AddColumn(TColumnSchema().Name("json").Type(NTi::Json()))

                .AddColumn(TColumnSchema().Name("date").Type(NTi::Date()))
                .AddColumn(TColumnSchema().Name("datetime").Type(NTi::Datetime()))
                .AddColumn(TColumnSchema().Name("timestamp").Type(NTi::Timestamp()))
                .AddColumn(TColumnSchema().Name("interval").Type(NTi::Interval()))

                .AddColumn(TColumnSchema().Name("null").Type(NTi::Null()))
                .AddColumn(TColumnSchema().Name("void").Type(NTi::Void()))
            )
        );

        writer->AddRow(row);
        writer->Finish();

        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdMapper());

        auto spec = *operation->GetAttributes().FullSpec;
        EXPECT_EQ(spec["mapper"]["input_format"].AsString(), "skiff");
    }
    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;
}


TEST(Operations, TestSkiffOperationHint)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
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
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper);

    const std::vector<TNode> expected = {TNode()("key", "foo")};
    std::vector<TNode> actual = ReadTable(client, workingDir + "/output");
    EXPECT_EQ(actual, expected);
}

TEST(Operations, TestComplexTypeMode)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;

    const auto structType = NTi::Struct({
        {"foo", NTi::String()},
        {"bar", NTi::Int64()},
    });
    const auto tableSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("value").Type(structType));

    const auto initialData = std::vector<TNode>{
        TNode::CreateMap({
            {
                "value",
                TNode::CreateMap({
                    {"foo", "foo-value"},
                    {"bar", 5},
                })
            }
        }),
    };

    {
        auto writer = client->CreateTableWriter<TNode>(TRichYPath(workingDir + "/input").Schema(tableSchema));

        for (const auto& row : initialData) {
            writer->AddRow(row);
        }

        writer->Finish();
    }

    client->Map(
        TMapOperationSpec()
            .InputFormatHints(TFormatHints().ComplexTypeMode(EComplexTypeMode::Positional))
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/intermediate"),
        new TIdMapper);

    {
        const std::vector<TNode> expected = {TNode()("value", TNode::CreateList({"foo-value", 5}))};
        std::vector<TNode> actual = ReadTable(client, workingDir + "/intermediate");
        EXPECT_EQ(actual, expected);
    }

    client->Map(
        TMapOperationSpec()
            .OutputFormatHints(TFormatHints().ComplexTypeMode(EComplexTypeMode::Positional))
            .AddInput<TNode>(workingDir + "/intermediate")
            .AddOutput<TNode>(TRichYPath(workingDir + "/output").Schema(tableSchema)),
        new TIdMapper);

    {
        std::vector<TNode> actual = ReadTable(client, workingDir + "/output");
        EXPECT_EQ(actual, initialData);
    }
}

TEST(Operations, TestSkiffOperationHintConfigurationConflict)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
            .Schema(TTableSchema()
                .Strict(true)
                .AddColumn(TColumnSchema().Name("key").Type(VT_STRING))
                .AddColumn(TColumnSchema().Name("value").Type(VT_STRING))));
        writer->AddRow(TNode()("key", "foo")("value", TNode::CreateEntity()));
        writer->Finish();
    }

    EXPECT_THROW(
        client->Map(
            TMapOperationSpec()
            .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdMapper),
        TApiUsageError);
}

void TestIncompleteReducer(ENodeReaderFormat nodeReaderFormat)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = nodeReaderFormat;

    auto inputPath = TRichYPath(workingDir + "/input")
        .Schema(TTableSchema()
            .Strict(true)
            .AddColumn(TColumnSchema().Name("key").Type(VT_INT64).SortOrder(SO_ASCENDING))
            .AddColumn(TColumnSchema().Name("value").Type(VT_INT64)));
    auto outputPath = TRichYPath(workingDir + "/output");
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
            EXPECT_TRUE(reader->IsValid());
            EXPECT_EQ(reader->GetRow(),
                TNode()
                    ("key", static_cast<i64>(index + 1))
                    ("sum", expectedValues[index]));
            reader->Next();
        }
        EXPECT_TRUE(!reader->IsValid());
    }
}

TEST(Operations, IncompleteReducer_Yson)
{
    TestIncompleteReducer(ENodeReaderFormat::Yson);
}

TEST(Operations, IncompleteReducer_Skiff)
{
    TestIncompleteReducer(ENodeReaderFormat::Skiff);
}

void TestRowIndices(ENodeReaderFormat nodeReaderFormat)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = nodeReaderFormat;

    TYPath inputTable = workingDir + "/input";
    TYPath outputTable = workingDir + "/output";

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
            .SortBy(TSortColumns().Add("foo")),
        new TMapperThatNumbersRows,
        new TIdReducer);

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson;
    {
        auto reader = client->CreateTableReader<TNode>(outputTable);
        for (int i = 3; i < 8; ++i) {
            EXPECT_TRUE(reader->IsValid());
            EXPECT_EQ(reader->GetRow(), TNode()("foo", i)("INDEX", static_cast<ui64>(i)));
            reader->Next();
        }
        EXPECT_TRUE(!reader->IsValid());
    }
}

TEST(Operations, RowIndices_Yson)
{
    TestRowIndices(ENodeReaderFormat::Yson);
}

TEST(Operations, RowIndices_Skiff)
{
    TestRowIndices(ENodeReaderFormat::Skiff);
}

template<class TInputRowType>
void TestRangeIndices(ENodeReaderFormat nodeReaderFormat)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = nodeReaderFormat;

    TYPath inputTable = workingDir + "/input";
    TYPath outputTable = workingDir + "/output";

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(inputTable)
                .Schema(TTableSchema().AddColumn("key", VT_STRING).AddColumn("value", VT_STRING)));
        for (size_t i = 0; i < 20; ++i) {
            writer->AddRow(TNode()("key", ToString(i))("value", ToString(i)));
        }
        writer->Finish();
    }

    auto path = TRichYPath(inputTable)
        .AddRange(TReadRange()
            .LowerLimit(TReadLimit().RowIndex(3))
            .UpperLimit(TReadLimit().RowIndex(8))
        )
        .AddRange(TReadRange()
            .LowerLimit(TReadLimit().RowIndex(10))
            .UpperLimit(TReadLimit().RowIndex(12))
        );

    client->Map(
        TMapOperationSpec()
            .Ordered(true)
            .AddInput<TInputRowType>(path)
            .template AddOutput<TNode>(outputTable),
        ::MakeIntrusive<TMapperThatWritesRowsAndRanges<TInputRowType>>(),
        TOperationOptions()
    );

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson;
    {

        auto reader = client->CreateTableReader<TNode>(outputTable);
        TVector<ui32> actualRangeIndices;
        TVector<ui64> actualRowIndices;
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->GetRow();
            actualRangeIndices.push_back(row["range_id"].AsUint64());
            actualRowIndices.push_back(row["row_id"].AsUint64());
        }
        const TVector<ui32> expectedRangeIndices = {
            0, 0, 0, 0, 0,
            1, 1,
        };
        const TVector<ui64> expectedRowIndices = {
            3, 4, 5, 6, 7,
            10, 11,
        };
        EXPECT_EQ(actualRangeIndices, expectedRangeIndices);
        EXPECT_EQ(actualRowIndices, expectedRowIndices);
    }
}

TEST(Operations, RangeIndices_Yson_TNode)
{
    TestRangeIndices<TNode>(ENodeReaderFormat::Yson);
}

TEST(Operations, RangeIndices_Skiff_TNode)
{
    TestRangeIndices<TNode>(ENodeReaderFormat::Skiff);
}

TEST(Operations, RangeIndices_TYaMRRow)
{
    TestRangeIndices<TYaMRRow>(ENodeReaderFormat::Yson);
}

TEST(Operations, RangeIndices_Protobuf)
{
    TestRangeIndices<TEmbeddedMessage>(ENodeReaderFormat::Yson);
}

TEST(Operations, OrderedDynamicTableReadLimits)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto schema = TNode()
        .Add(TNode()("name", "key")("type", "int64"))
        .Add(TNode()("name", "value")("type", "int64"));
    const auto inputPath = workingDir + "/input";
    const auto outputPath = workingDir + "/output";
    client->Create(inputPath, NT_TABLE, TCreateOptions().Attributes(
        TNode()("dynamic", true)("schema", schema)));

    client->ReshardTable(inputPath, 2);
    client->MountTable(inputPath);
    WaitForTabletsState(client, inputPath, TS_MOUNTED, TWaitForTabletsStateOptions()
        .Timeout(TDuration::Seconds(30))
        .CheckInterval(TDuration::MilliSeconds(50)));

    client->InsertRows(inputPath, {TNode()("key", 1)("value", 2)("$tablet_index", 0)});
    client->InsertRows(inputPath, {TNode()("key", 3)("value", 4)("$tablet_index", 1)});
    client->InsertRows(inputPath, {TNode()("key", 5)("value", 6)("$tablet_index", 1)});

    client->FreezeTable(inputPath);
    WaitForTabletsState(client, inputPath, TS_FROZEN, TWaitForTabletsStateOptions()
        .Timeout(TDuration::Seconds(30))
        .CheckInterval(TDuration::MilliSeconds(50)));
    client->UnfreezeTable(inputPath);
    WaitForTabletsState(client, inputPath, TS_MOUNTED, TWaitForTabletsStateOptions()
        .Timeout(TDuration::Seconds(30))
        .CheckInterval(TDuration::MilliSeconds(50)));

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;

    TRichYPath path;
    auto runOperation = [&] () {
        auto spec = TNode::CreateMap();
        spec["job_io"]["control_attributes"]["enable_tablet_index"] = TNode(true);

        return client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(path)
                .AddOutput<TNode>(outputPath),
            ::MakeIntrusive<TMapperForOrderedDynamicTables>(),
            TOperationOptions()
                .Spec(std::move(spec)));
    };

    // We cannot specify row index without tablet index.
    path = TRichYPath(inputPath)
        .AddRange(TReadRange()
            .LowerLimit(TReadLimit().RowIndex(0)));
    EXPECT_THROW(
        runOperation(),
        TOperationFailedError);

    path = TRichYPath(inputPath)
        .AddRange(TReadRange()
            .UpperLimit(TReadLimit().TabletIndex(0).RowIndex(0))
        )
        .AddRange(TReadRange()
            .UpperLimit(TReadLimit().TabletIndex(0))
        )
        .AddRange(TReadRange()
            .LowerLimit(TReadLimit().TabletIndex(1))
        )
        .AddRange(TReadRange()
            .LowerLimit(TReadLimit().TabletIndex(0).RowIndex(0))
            .UpperLimit(TReadLimit().TabletIndex(1))
        )
        .AddRange(TReadRange()
            .LowerLimit(TReadLimit().TabletIndex(0).RowIndex(0))
            .UpperLimit(TReadLimit().TabletIndex(1).RowIndex(1))
        )
        .AddRange(TReadRange()
            .LowerLimit(TReadLimit().TabletIndex(1))
            .UpperLimit(TReadLimit().TabletIndex(1).RowIndex(1))
        );
    EXPECT_NO_THROW(runOperation());

    auto sorted = [] (TVector<TNode>&& nodes) {
        auto result = std::move(nodes);
        std::sort(result.begin(), result.end(), [] (const TNode& lhs, const TNode& rhs) {
            auto getKey = [](const TNode& value) {
                return std::tuple(
                    value["range_index"].IntCast<i64>(),
                    value["tablet_index"].IntCast<i64>(),
                    value["row_index"].IntCast<i64>())
                ;
            };
            return getKey(lhs) < getKey(rhs);
        });
        return result;
    };

    const TVector<TNode> expected = sorted({
        // Range 0 is empty

        // Range 1 is empty

        TNode()("range_index", 2u)("tablet_index", 1)("row_index", 0u)("key", 3)("value", 4),
        TNode()("range_index", 2u)("tablet_index", 1)("row_index", 1u)("key", 5)("value", 6),

        TNode()("range_index", 3u)("tablet_index", 0)("row_index", 0u)("key", 1)("value", 2),

        TNode()("range_index", 4u)("tablet_index", 0)("row_index", 0u)("key", 1)("value", 2),
        TNode()("range_index", 4u)("tablet_index", 1)("row_index", 0u)("key", 3)("value", 4),

        TNode()("range_index", 5u)("tablet_index", 1)("row_index", 0u)("key", 3)("value", 4),
    });
    const auto actual = sorted(ReadTable(client, outputPath));

    EXPECT_EQ(expected, actual);
}

TEST(Operations, SkiffForInputQuery)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;

    TYPath inputTable = workingDir + "/input";
    TYPath outputTable = workingDir + "/output";

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

    EXPECT_THROW(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(outputTable),
            new TMapperThatNumbersRows,
            TOperationOptions()
                .Spec(TNode()("input_query", "foo AS foo WHERE foo > 5"))),
        TApiUsageError);
}

TEST(Operations, SkiffForDynamicTables)
{
    TTabletFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto schema = TNode()
        .Add(TNode()("name", "key")("type", "string"))
        .Add(TNode()("name", "value")("type", "int64"));
    const auto inputPath = workingDir + "/input";
    const auto outputPath = workingDir + "/output";
    client->Create(inputPath, NT_TABLE, TCreateOptions().Attributes(
        TNode()("dynamic", true)("schema", schema)));
    client->MountTable(inputPath);
    WaitForTabletsState(client, inputPath, TS_MOUNTED, TWaitForTabletsStateOptions()
        .Timeout(TDuration::Seconds(30))
        .CheckInterval(TDuration::MilliSeconds(50)));
    client->InsertRows(inputPath, {TNode()("key", "key")("value", 33)});

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;
    EXPECT_NO_THROW(
        client->Map(
            TMapOperationSpec()
                .Ordered(true)
                .AddInput<TNode>(inputPath)
                .AddOutput<TNode>(outputPath),
            new TIdMapper));

    TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;
    EXPECT_THROW(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputPath)
                .AddOutput<TNode>(outputPath),
            new TIdMapper),
        yexception);
}

TEST(Operations, FileCacheModes)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    client->Create(workingDir + "/file_storage", NT_MAP);
    CreateTableWithFooColumn(client, workingDir + "/input");

    TTempFile tempFile(MakeTempName());
    {
        TOFStream os(tempFile.Name());
        // Create a file with unique contents to get cache miss
        os << CreateGuidAsString();
    }

    auto tx = client->StartTransaction();

    EXPECT_THROW(
        tx->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output")
                .MapperSpec(TUserJobSpec()
                    .AddLocalFile(tempFile.Name())),
            new TIdMapper,
            TOperationOptions()
                .FileStorage(workingDir + "/file_storage")
                .FileStorageTransactionId(tx->GetId())),
        TApiUsageError);

    EXPECT_NO_THROW(
        tx->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output")
                .MapperSpec(TUserJobSpec()
                    .AddLocalFile(tempFile.Name())),
            new TIdMapper,
            TOperationOptions()
                .FileStorage(workingDir + "/file_storage")
                .FileStorageTransactionId(tx->GetId())
                .FileCacheMode(TOperationOptions::EFileCacheMode::CachelessRandomPathUpload)));
}

TEST(Operations, CacheCleanedWhenOperationStartWasRetried)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TTempFile tempFile(MakeTempName());
    {
        TOFStream os(tempFile.Name());
        // Create a file with unique contents to get cache miss
        os << CreateGuidAsString();
    }

    client->Create(
        "",
        NT_SCHEDULER_POOL,
        TCreateOptions().Attributes(NYT::TNode()
            ("name", "testing")
            ("pool_tree", "default")
            ("max_running_operation_count", 1)
            ("max_pending_operation_count", 1)
        ));

    Y_DEFER {
        client->Remove("//sys/pools/testing");
    };

    auto sleepingOp = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TSleepingMapper(TDuration::Minutes(10)),
        TOperationOptions()
            .Spec(TNode()("pool", "testing"))
            .Wait(false));

    Y_DEFER {
        if (sleepingOp->GetBriefState() == EOperationBriefState::InProgress) {
            sleepingOp->AbortOperation();
        }
    };

    auto runMap = [&] {
        auto opWithFile = client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output_1")
                .MapperSpec(TUserJobSpec()
                    .AddLocalFile(tempFile.Name(), TAddLocalFileOptions().PathInJob("myfile"))),
            new TMapperThatChecksFile("myfile"),
            TOperationOptions()
                .Spec(TNode()("pool", "testing")));
    };

    auto threadPool = SystemThreadFactory();
    auto thread = threadPool->Run(runMap);

    auto md5 = MD5::File(tempFile.Name());
    EXPECT_NO_THROW(
        WaitForPredicate([&] {
            // Wait for the file to appear in cache.
            auto path = client->GetFileFromCache(md5, TConfig::Get()->RemoteTempFilesDirectory + "/new_cache");
            return path.Defined();
        }));

    auto path = client->GetFileFromCache(md5, TConfig::Get()->RemoteTempFilesDirectory + "/new_cache");
    EXPECT_TRUE(path);

    EXPECT_NO_THROW(
        WaitForPredicate([&] {
            // Wait for the lock to be taken.
            return !client->Get(*path + "/@locks").Empty();
        }));

    // Simulate cache cleaning.
    client->Remove(*path);

    sleepingOp->AbortOperation();

    EXPECT_NO_THROW(thread->Join());
}

TEST(Operations, CacheCleanedWhenOperationWasPending)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    CreateTableWithFooColumn(client, workingDir + "/input");

    TTempFile tempFile(MakeTempName());
    {
        TOFStream os(tempFile.Name());
        // Create a file with unique contents to get cache miss
        os << CreateGuidAsString();
    }

    client->Create(
        "",
        NT_SCHEDULER_POOL,
        TCreateOptions().Attributes(NYT::TNode()
            ("name", "testing")
            ("pool_tree", "default")
            ("max_running_operation_count", 1)));

    Y_DEFER {
        client->Remove("//sys/pools/testing");
    };

    auto sleepingOp = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TSleepingMapper(TDuration::Minutes(10)),
        TOperationOptions()
            .Spec(TNode()("pool", "testing"))
            .Wait(false));

    Y_DEFER {
        if (sleepingOp->GetBriefState() == EOperationBriefState::InProgress) {
            sleepingOp->AbortOperation();
        }
    };

    auto opWithFile = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output_1")
            .MapperSpec(TUserJobSpec()
                .AddLocalFile(tempFile.Name(), TAddLocalFileOptions().PathInJob("myfile"))),
        new TMapperThatChecksFile("myfile"),
        TOperationOptions()
            .Spec(TNode()("pool", "testing"))
            .Wait(false));

    WaitOperationHasState(opWithFile, "pending");

    auto md5 = MD5::File(tempFile.Name());
    auto path = client->GetFileFromCache(md5, TConfig::Get()->RemoteTempFilesDirectory + "/new_cache");
    EXPECT_TRUE(path);

    // Simulate cache cleaning.
    client->Remove(*path);

    sleepingOp->AbortOperation();
    EXPECT_NO_THROW(opWithFile->Watch().GetValueSync());
}

TEST(Operations, RetryLockConflict)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    CreateTableWithFooColumn(client, workingDir + "/input");

    TTempFile tempFile(MakeTempName());
    {
        TOFStream os(tempFile.Name());
        // Create a file with unique contents to get cache miss
        os << CreateGuidAsString();
    }

    auto runMap = [&] {
        auto tx = client->StartTransaction();
        tx->Map(
            TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output_" + CreateGuidAsString())
                .MapperSpec(TUserJobSpec()
                    .AddLocalFile(tempFile.Name())),
            new TAlwaysFailingMapper, // No exception here because of '.Wait(false)'.
            TOperationOptions()
                .Wait(false));
    };

    auto threadPool = SystemThreadFactory();
    TVector<TAutoPtr<IThreadFactory::IThread>> threads;
    // Run many concurrent threads to get lock conflict in 'put_file_to_cache'
    // with high probability.
    for (int i = 0; i < 10; ++i) {
        threads.push_back(threadPool->Run(runMap));
    }
    for (auto& t : threads) {
        t->Join();
    }
}

TEST(Operations, Vanilla)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TTempFile tempFile(MakeTempName());
    Chmod(tempFile.Name().c_str(), 0777);

    TString message = "Hello world!";
    ui64 firstJobCount = 2, secondJobCount = 3;

    client->RunVanilla(TVanillaOperationSpec()
        .AddTask(TVanillaTask()
            .Name("first")
            .Job(new TVanillaAppendingToFile(tempFile.Name(), message))
            .JobCount(firstJobCount))
        .AddTask(TVanillaTask()
            .Name("second")
            .Job(new TVanillaAppendingToFile(tempFile.Name(), message))
            .JobCount(secondJobCount)));

    TIFStream stream(tempFile.Name());
    EXPECT_EQ(stream.ReadAll().size(), (firstJobCount + secondJobCount) * message.size());
}

TEST(Operations, VanillaTableOutput)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto outputTable1 = TRichYPath(workingDir + "/output1");
    auto outputTable2 = TRichYPath(workingDir + "/output2");

    client->RunVanilla(TVanillaOperationSpec()
        .AddTask(TVanillaTask()
            .AddOutput<TNode>(outputTable1)
            .AddOutput<TNode>(outputTable2)
            .Job(new TVanillaWithTableOutput)
            .JobCount(1)
            .Name("vanilla")));

    TVector<TNode> expected1 = {
        TNode()("first", 0)("second", 0),
        TNode()("first", 1)("second", 2)
    };
    TVector<TNode> expected2 = {
        TNode()("first", 3)("second", 4),
        TNode()("first", 0)("second", 0)
    };
    auto actual1 = ReadTable(client, outputTable1.Path_);
    EXPECT_EQ(expected1, actual1);
    auto actual2 = ReadTable(client, outputTable2.Path_);
    EXPECT_EQ(expected2, actual2);
}

TEST(Operations, FailingVanilla)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TYPath stderrPath = workingDir + "/stderr";

    client->Create(stderrPath, NT_TABLE);

    EXPECT_THROW(
        client->RunVanilla(TVanillaOperationSpec()
            .AddTask(TVanillaTask()
                .Name("task")
                .Job(new TFailingVanilla())
                .JobCount(2))
            .StderrTablePath(stderrPath)
            .MaxFailedJobCount(10)),
        TOperationFailedError);

    EXPECT_NE(client->Get(stderrPath + "/@row_count"), 0);
}

TEST(Operations, VanillaOutputTableCountCheck)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto outputTable = TRichYPath(workingDir + "/output");

    TTempFile tempFile(MakeTempName());
    Chmod(tempFile.Name().c_str(), 0777);

    TString message = "Hello world!";

    EXPECT_THROW(
        client->RunVanilla(TVanillaOperationSpec()
            .AddTask(TVanillaTask()
                .Job(new TVanillaWithTableOutput)
                .JobCount(1)
                .Name("vanilla"))),
        TApiUsageError);

    EXPECT_THROW(
        client->RunVanilla(TVanillaOperationSpec()
            .AddTask(TVanillaTask()
                .Name("first")
                .Job(new TVanillaAppendingToFile(tempFile.Name(), message))
                .JobCount(1))
            .AddTask(TVanillaTask()
                .Name("second")
                .Job(new TVanillaAppendingToFile(tempFile.Name(), message))
                .JobCount(1)
                .AddOutput<TNode>(outputTable))),
        TApiUsageError);
}

// TODO(levysotsky): Enable this test when packages are updated.
void Descending()
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString inputTable = workingDir + "/table";
    auto sortBy = TSortColumns("key1", (TSortColumn("key2", ESortOrder::SO_DESCENDING)), "key3");

    auto getSortedBy = [&](const TString& table) {
        TSortColumns columns;
        auto schema = client->Get(table + "/@schema");
        for (const auto& column : schema.AsList()) {
            columns.Add(TSortColumn(column["name"].AsString(), ::FromString<ESortOrder>(column["sort_order"].AsString())));
        }
        return columns;
    };

    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("key1", "a")("key2", "b")("key3", "c")("value", "x"));
        writer->AddRow(TNode()("key1", "a")("key2", "b")("key3", "d")("value", "xx"));
        writer->AddRow(TNode()("key1", "a")("key2", "c")("key3", "a")("value", "xxx"));
        writer->AddRow(TNode()("key1", "b")("key2", "a")("key3", "a")("value", "xxxx"));
        writer->Finish();
    }

    auto outputTable = workingDir + "/output";
    client->Sort(TSortOperationSpec()
        .AddInput(inputTable)
        .AddInput(inputTable)
        .Output(outputTable)
        .SortBy(sortBy));

    EXPECT_EQ(getSortedBy(outputTable).Parts_, sortBy.Parts_);
    EXPECT_EQ(
        client->Get(outputTable + "/@row_count").AsInt64(),
        2 * client->Get(inputTable + "/@row_count").AsInt64());
}

TEST(Operations, LazySort)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString inputTable = workingDir + "/table";
    auto initialSortedBy = TSortColumns().Add("key1").Add("key2").Add("key3");

    auto getSortedBy = [&](const TString& table) {
        TSortColumns columns;
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
        auto prefixColumns = TSortColumns().Add("key1").Add("key2");
        TString outputTable = workingDir + "/output";
        auto operation = LazySort(
            client,
            TSortOperationSpec()
                .AddInput(inputTable)
                .AddInput(inputTable)
                .Output(outputTable)
                .SortBy(prefixColumns));

        EXPECT_NE(operation, nullptr);
        // It must be merge because input tables are already sorted
        EXPECT_EQ(getType(operation), EOperationType::Merge);
        EXPECT_EQ(getSortedBy(outputTable).Parts_, prefixColumns.Parts_);
        EXPECT_EQ(
            client->Get(outputTable + "/@row_count").AsInt64(),
            2 * client->Get(inputTable + "/@row_count").AsInt64());
    }
    {
        auto nonPrefixColumns = TSortColumns().Add("key2").Add("key3");
        TString outputTable = workingDir + "/output";
        auto operation = LazySort(
            client,
            TSortOperationSpec()
                .AddInput(inputTable)
                .Output(outputTable)
                .SortBy(nonPrefixColumns));
        EXPECT_NE(operation, nullptr);
        EXPECT_EQ(getType(operation), EOperationType::Sort);
        EXPECT_EQ(getSortedBy(outputTable).Parts_, nonPrefixColumns.Parts_);
    }
}

TEST(Operations, FormatHint)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
            .Schema(TTableSchema()
                .Strict(true)
                .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))
                .AddColumn(TColumnSchema().Name("value").Type(VT_STRING))));

        writer->AddRow(TNode()("key", "foo")("value", TNode::CreateEntity()));
        writer->Finish();
    }
    const TVector<TNode> expected = {TNode()("key", "foo")};
    auto readOutputAndRemove = [&] () {
        auto result = ReadTable(client, workingDir + "/output");
        client->Remove(workingDir + "/output");
        return result;
    };

    client->Map(
        TMapOperationSpec()
        .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper);
    EXPECT_EQ(readOutputAndRemove(), expected);

    client->Reduce(
        TReduceOperationSpec()
        .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
        .ReduceBy("key")
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output"),
        new TIdReducer);
    EXPECT_EQ(readOutputAndRemove(), expected);

    client->MapReduce(
        TMapReduceOperationSpec()
        .ReduceBy("key")
        .MapperFormatHints(TUserJobFormatHints().InputFormatHints(TFormatHints().SkipNullValuesForTNode(true)))
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper,
        new TIdReducer);
    EXPECT_EQ(readOutputAndRemove(), expected);

    client->MapReduce(
        TMapReduceOperationSpec()
        .ReduceBy("key")
        .ReducerFormatHints(TUserJobFormatHints().InputFormatHints(TFormatHints().SkipNullValuesForTNode(true)))
        .AddInput<TNode>(workingDir + "/input")
        .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper,
        new TIdReducer);
    EXPECT_EQ(readOutputAndRemove(), expected);
}

TEST(Operations, AttachOperation)
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
        new TSleepingMapper(TDuration::Seconds(100)),
        TOperationOptions().Wait(false));

    auto attached = client->AttachOperation(operation->GetId());

    attached->AbortOperation();

    EXPECT_EQ(operation->GetBriefState(), EOperationBriefState::Aborted);
}

TEST(Operations, AttachInexistingOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    try {
        client->AttachOperation(GetGuid("1-2-3-4"));
        FAIL() << "exception expected to be thrown";
    } catch (const TErrorResponse& e) {
        e.GetError().ContainsErrorCode(1915); // TODO: need named error code
    }
}

TEST(Operations, CrossTransactionMerge)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx1 = client->StartTransaction();
    auto tx2 = client->StartTransaction();

    {
        auto writer = tx1->CreateTableWriter<TNode>(workingDir + "/input1");
        writer->AddRow(TNode()("row", "foo"));
        writer->Finish();
    }
    {
        auto writer = tx2->CreateTableWriter<TNode>(workingDir + "/input2");
        writer->AddRow(TNode()("row", "bar"));
        writer->Finish();
    }
    client->Merge(
        TMergeOperationSpec()
        .AddInput(
            TRichYPath(workingDir + "/input1")
            .TransactionId(tx1->GetId()))
        .AddInput(
            TRichYPath(workingDir + "/input2")
            .TransactionId(tx2->GetId()))
        .Output(workingDir + "/output"));
    tx1->Abort();
    tx2->Abort();

    TVector<TNode> expected = {
        TNode()("row", "foo"),
        TNode()("row", "bar"),
    };
    auto actual = ReadTable(client, workingDir + "/output");
    EXPECT_EQ(expected, actual);
}

TEST(Operations, OutputTableCounter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
                .Schema(TTableSchema()
                            .Strict(true)
                            .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))
                            .AddColumn(TColumnSchema().Name("value").Type(VT_STRING))));
        writer->AddRow(TNode()("key", "key1")("value", "value1"));
        writer->Finish();
    }

    {
        client->Reduce(
            TReduceOperationSpec()
                .ReduceBy("key")
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output1"),
            new TReducerThatCountsOutputTables());

            auto result = ReadTable(client, workingDir + "/output1");
            const auto expected = TVector<TNode>{TNode()("result", 1)};
            EXPECT_EQ(result, expected);
    }

    {
        client->Reduce(
            TReduceOperationSpec()
                .ReduceBy("key")
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output1")
                .AddOutput<TNode>(workingDir + "/output2"),
            new TReducerThatCountsOutputTables());

            auto actual = ReadTable(client, workingDir + "/output1");
            const auto expected = TVector<TNode>{TNode()("result", 2)};
            EXPECT_EQ(actual, expected);
    }
}

TEST(Operations, AllocatedPorts)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TTempFile tempFile(MakeTempName());
    Chmod(tempFile.Name().c_str(), 0777);

    const ui16 portCount = 7;

    client->RunVanilla(TVanillaOperationSpec()
        .AddTask(TVanillaTask()
            .Name("first")
            .Job(new TVanillaWithPorts(tempFile.Name(), portCount))
            .Spec(TUserJobSpec{}.PortCount(portCount))
            .JobCount(1)));

    TFileInput stream(tempFile.Name());
    TString line;
    for ([[maybe_unused]] const auto _: xrange(portCount)) {
        EXPECT_TRUE(stream.ReadLine(line));
        const auto port = FromString<ui16>(line);
        EXPECT_GT(port, 1023);
    }
    EXPECT_TRUE(!stream.ReadLine(line));
}

//COMPAT(hiddenpath): Enable test when consumer manager option will be shipped to production
TEST(Operations, DISABLED_UnrecognizedSpecWarnings)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto oldLogger = GetLogger();
    Y_DEFER {
        SetLogger(oldLogger);
    };

    TStringStream stream;
    SetLogger(new TStreamTeeLogger(ILogger::INFO, &stream, oldLogger));

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "baz"));
        writer->Finish();
    }

    auto operation = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
        new TIdMapper,
        TOperationOptions()
            .Spec(TNode()
                ("mapper", TNode()("blah1", 1))
                ("blah2", 2)));

    TNode unrecognizedSpec;
    TStringBuf prefix = "WARNING! Unrecognized spec for operation";
    for (TStringBuf line : StringSplitter(stream.Str()).Split('\n')) {
        if (line.StartsWith(prefix)) {
            unrecognizedSpec = NodeFromYsonString(line.After(':'));
            break;
        }
    }

    EXPECT_EQ(unrecognizedSpec.GetType(), TNode::Map);
    EXPECT_EQ(
        unrecognizedSpec,
        TNode()
            ("mapper", TNode()("blah1", 1))
            ("blah2", 2));
}

TEST(Operations, NewMapReduceOverloads)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input")
            .Schema(TTableSchema()
                .Strict(true)
                .AddColumn(TColumnSchema().Name("key").Type(VT_STRING).SortOrder(SO_ASCENDING))
                .AddColumn(TColumnSchema().Name("value").Type(VT_STRING))));

        writer->AddRow(TNode()("key", "foo")("value", "7"));
        writer->Finish();
    }
    const TVector<TNode> expected = {TNode()("key", "foo")("value", "7")};
    auto readOutputAndRemove = [&] () {
        auto result = ReadTable(client, workingDir + "/output");
        client->Remove(workingDir + "/output");
        return result;
    };

    client->Map(
        new TIdMapper,
        workingDir + "/input",
        workingDir + "/output");
    EXPECT_EQ(readOutputAndRemove(), expected);

    client->Reduce(
        new TIdReducer,
        workingDir + "/input",
        workingDir + "/output",
        "key");
    EXPECT_EQ(readOutputAndRemove(), expected);

    client->MapReduce(
        new TIdMapper,
        new TIdReducer,
        workingDir + "/input",
        workingDir + "/output",
        "key");
    EXPECT_EQ(readOutputAndRemove(), expected);

    client->MapReduce(
        new TIdMapper,
        new TIdReducer,
        new TIdReducer,
        workingDir + "/input",
        workingDir + "/output",
        "key");
    EXPECT_EQ(readOutputAndRemove(), expected);
}

TEST(Operations, NewSortOverload)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath(workingDir + "/input"));

        writer->AddRow(TNode()("key", "foo"));
        writer->AddRow(TNode()("key", "bar"));
        writer->Finish();
    }
    const TVector<TNode> expected = {TNode()("key", "bar"), TNode()("key", "foo")};

    client->Sort(
        workingDir + "/input",
        workingDir + "/output",
        "key");
    EXPECT_EQ(ReadTable(client, workingDir + "/output"), expected);
}

TEST(Operations, OperationTimeout)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto writer = client->CreateTableWriter<TNode>(inputTable);
    writer->AddRow(TNode()("key", 1));
    writer->Finish();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(workingDir + "/output")
                .TimeLimit(TDuration::Seconds(2)),
            new TSleepingMapper(TDuration::Seconds(3))),
        TOperationFailedError,
        "Operation is running for too long");
}

TEST(Operations, JobTimeout)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto writer = client->CreateTableWriter<TNode>(inputTable);
    writer->AddRow(TNode()("key", 1));
    writer->Finish();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable)
                .AddOutput<TNode>(workingDir + "/output")
                .MapperSpec(TUserJobSpec().JobTimeLimit(TDuration::Seconds(2))),
            new TSleepingMapper(TDuration::Seconds(3))),
        TOperationFailedError,
        "Job time limit exceeded");
}

TEST(Operations, QLFilter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
    auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputTable)
        .Schema(TTableSchema().AddColumn("foo", VT_INT64)));
    const int n = 10, k = 5;
    for (int i = 0; i < n; ++i) {
        writer->AddRow(TNode()("foo", i));
    }
    writer->Finish();

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputTable)
            .AddOutput<TNode>(outputTable),
        new TIdMapper(),
        TOperationOptions()
            .Spec(TNode()("input_query", "foo AS foo WHERE foo >= " + ToString(k))));

    auto reader = client->CreateTableReader<TNode>(outputTable);
    {
        int i = k;
        for (const auto& cursor : *reader) {
            EXPECT_EQ(cursor.GetRow(), TNode()("foo", i));
            ++i;
        }
    }
}

TEST(Operations, QLAndColumnFilter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
    auto writer = client->CreateTableWriter<TNode>(TRichYPath(inputTable)
        .Schema(TTableSchema().AddColumn("foo", VT_INT64)));
    const int n = 10, k = 5;
    for (int i = 0; i < n; ++i) {
        writer->AddRow(TNode()("foo", i));
    }
    writer->Finish();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        client->Map(
            TMapOperationSpec()
                .AddInput<TNode>(inputTable.Columns("key"))
                .AddOutput<TNode>(outputTable),
            new TIdMapper(),
            TOperationOptions()
                .Spec(TNode()("input_query", "foo AS foo WHERE foo >= " + ToString(k)))),
        TOperationFailedError,
        "Column filter and QL filter cannot appear in the same operation");
}

TEST(Operations, CommandRawJob)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto outputTable = TRichYPath(workingDir + "/output");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("a", "foo")("b", "bar"));
        writer->AddRow(TNode()("a", "koo")("b", "kindzadza"));
        writer->Finish();
    }
    client->RawMap(
        TRawMapOperationSpec()
            .AddInput(inputTable)
            .AddOutput(outputTable)
            .Format(TFormat::Json()),
        new TCommandRawJob("grep dza"));

    TVector<TNode> rows = ReadTable(client, outputTable.Path_);
    EXPECT_EQ(rows, TVector{TNode()("a", "koo")("b", "kindzadza")});
}

TEST(Operations, CommandVanillaJob)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TTempFile tempFile(MakeTempName());
    Chmod(tempFile.Name().c_str(), 0777);

    client->RunVanilla(
        TVanillaOperationSpec()
            .AddTask(TVanillaTask()
                .Name("Hello world")
                .Job(new TCommandVanillaJob("echo \"Hello world!\" > " + tempFile.Name()))
                .JobCount(1)));

    TIFStream is(tempFile.Name());
    EXPECT_EQ(is.ReadAll(), "Hello world!\n");
}

TEST(Operations, ProtobufColumnFilter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    const auto inputTable = TRichYPath(workingDir + "/input");
    const auto outputTable = TRichYPath(workingDir + "/output");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("bar", 1)("foo", 1)("Host", "ya.ru")("Path", "/")("HttpCode", 404)("spam", 1));
        writer->Finish();
    }

    client->Sort(inputTable, inputTable, {"HttpCode", "foo"});

    auto check = [&](IOperationPtr op, const THashSet<TString>& expectedColumns, const TVector<TNode>& expectedRows) {
        auto rows = ReadTable(client, outputTable.Path_);
        EXPECT_EQ(rows, expectedRows);

        TVector<TRichYPath> paths;
        Deserialize(paths, op->GetAttributes().Spec->At("input_table_paths"));
        for (const auto& path : paths) {
            THashSet<TString> columns;
            if (path.Columns_) {
                columns.insert(path.Columns_->Parts_.begin(), path.Columns_->Parts_.end());
            }
            EXPECT_EQ(columns, expectedColumns);
        }
    };

    check(
        client->MapReduce(
            TMapReduceOperationSpec()
                .AddInput<TUrlRow>(inputTable)
                .AddOutput<TUrlRow>(outputTable)
                .ReduceBy({"HttpCode", "foo"})
                .SortBy({"HttpCode", "foo", "bar"}),
            new TUrlRowIdMapper,
            new TUrlRowIdReducer),
        {"HttpCode", "Host", "Path", "foo", "bar"},
        {TNode()("Host", "ya.ru")("Path", "/")("HttpCode", 404)});

    auto inputTableFiltered = TRichYPath(inputTable).Columns({"HttpCode", "Path"});
    check(
        client->MapReduce(
            TMapReduceOperationSpec()
                .AddInput<TUrlRow>(inputTableFiltered)
                .AddOutput<TUrlRow>(outputTable)
                .ReduceBy({"HttpCode", "foo"}),
            nullptr,
            new TUrlRowIdReducer),
        {"HttpCode", "Path"},
        {TNode()("HttpCode", 404)("Path", "/")});

    check(
        client->Map(
            TMapOperationSpec()
                .AddInput<TUrlRow>(inputTable)
                .AddOutput<TUrlRow>(outputTable),
            new TUrlRowIdMapper),
        {"HttpCode", "Path", "Host"},
        {TNode()("HttpCode", 404)("Path", "/")("Host", "ya.ru")});

    check(
        client->Reduce(
            TReduceOperationSpec()
                .AddInput<TUrlRow>(inputTable)
                .AddOutput<TUrlRow>(outputTable)
                .ReduceBy({"HttpCode", "foo"}),
            new TUrlRowIdReducer),
        {"HttpCode", "Path", "Host", "foo"},
        {TNode()("HttpCode", 404)("Path", "/")("Host", "ya.ru")});

    check(
        client->JoinReduce(
            TJoinReduceOperationSpec()
                .AddInput<TUrlRow>(TRichYPath(inputTable).Foreign(true))
                .AddInput<TUrlRow>(inputTable)
                .AddOutput<TUrlRow>(outputTable)
                .JoinBy({"HttpCode", "foo"}),
            new TUrlRowIdReducer),
        {"HttpCode", "Path", "Host", "foo"},
        {
            TNode()("HttpCode", 404)("Path", "/")("Host", "ya.ru"),
            TNode()("HttpCode", 404)("Path", "/")("Host", "ya.ru"),
        });

    const auto dynamicTable = workingDir + "/dynamic_input";
    const auto schema = TTableSchema()
        .AddColumn(TColumnSchema().Name("string_1").Type(VT_STRING).SortOrder(SO_ASCENDING))
        .AddColumn(TColumnSchema().Name("uint32_2").Type(VT_UINT32))
        .AddColumn(TColumnSchema().Name("extra").Type(VT_STRING));
    client->Create(
        dynamicTable,
        NT_TABLE,
        TCreateOptions().Attributes(TNode()("dynamic", true)("schema", schema.ToNode())));

    client->MountTable(dynamicTable);
    WaitForTabletsState(client, dynamicTable, TS_MOUNTED);
    client->InsertRows(dynamicTable, {TNode()("string_1", "str")("uint32_2", 1U)("extra", "extra")});
    client->UnmountTable(dynamicTable);
    WaitForTabletsState(client, dynamicTable, TS_UNMOUNTED);

    // Note that column filter is empty for a dynamic table.
    check(
        client->Map(
            TMapOperationSpec()
                .AddInput<TRowVer2>(dynamicTable)
                .AddOutput<TRowVer2>(outputTable),
            new TIdTRowVer2Mapper),
        {},
        {TNode()("string_1", "str")("uint32_2", 1U)});
}


TEST(Operations, NoOutputOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = workingDir + "/input";

    TVector<TNode> data = {
        TNode()("foo", "bar"),
    };
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        for (const auto& row : data) {
            writer->AddRow(row);
        }
        writer->Finish();
    }
    auto operation = client->Map(new TMapperThatWritesStderr, {inputTable}, {});
    auto jobStatistics = operation->GetJobStatistics();
    EXPECT_TRUE(jobStatistics.GetStatistics("time/total").Max().Defined());
}

TEST(Operations, FuturesAfterPreparationFailed)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    for (auto mode : {
        TOperationOptions::EStartOperationMode::AsyncPrepare,
        TOperationOptions::EStartOperationMode::AsyncStart,
    }) {
        auto operation = client->Map(
            new TIdMapper,
            {workingDir + "/non-existent-table"},
            {},
            TMapOperationSpec(),
            TOperationOptions().StartOperationMode(mode));
        EXPECT_THROW(operation->GetPreparedFuture().GetValueSync(), TErrorResponse);
        EXPECT_THROW(operation->GetStartedFuture().GetValueSync(), TErrorResponse);
        EXPECT_THROW(operation->Watch().GetValueSync(), TErrorResponse);
        if (mode == TOperationOptions::EStartOperationMode::AsyncPrepare) {
            EXPECT_THROW(operation->Start(), TErrorResponse);
        }
    }
}

TEST(Operations, FuturesAfterStartFailed)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->StartOperationRetryCount = 3;
    TConfig::Get()->StartOperationRetryInterval = TDuration::MilliSeconds(0);

    CreateTableWithFooColumn(client, workingDir + "/input");

    for (auto mode : {
        TOperationOptions::EStartOperationMode::AsyncPrepare,
        TOperationOptions::EStartOperationMode::AsyncStart,
    }) {
        auto outage = TAbortableHttpResponse::StartOutage("/start_op");
        auto operation = client->Map(
            new TIdMapper,
            {workingDir + "/input"},
            {workingDir + "/output"},
            TMapOperationSpec(),
            TOperationOptions().StartOperationMode(mode));
        operation->GetPreparedFuture().GetValueSync();
        if (mode == TOperationOptions::EStartOperationMode::AsyncPrepare) {
            EXPECT_THROW(operation->Start(), TAbortedForTestPurpose);
        }
        EXPECT_THROW(operation->GetStartedFuture().GetValueSync(), TAbortedForTestPurpose);
        EXPECT_THROW(operation->Watch().GetValueSync(), TAbortedForTestPurpose);
    }
}

TEST(Operations, FuturesAfterOperationFailed)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    auto writer = client->CreateTableWriter<TNode>(inputTable);
    writer->AddRow(TNode()("key", 1));
    writer->Finish();

    for (auto mode : {
        TOperationOptions::EStartOperationMode::AsyncPrepare,
        TOperationOptions::EStartOperationMode::AsyncStart,
        TOperationOptions::EStartOperationMode::SyncStart,
    }) {
        auto operation = client->Map(
            new TSleepingMapper(TDuration::Seconds(3)),
            {inputTable},
            {workingDir + "/output"},
            TMapOperationSpec().TimeLimit(TDuration::Seconds(2)),
            TOperationOptions().StartOperationMode(mode));
        operation->GetPreparedFuture().GetValueSync();
        if (mode == TOperationOptions::EStartOperationMode::AsyncPrepare) {
            operation->Start();
        }
        operation->GetStartedFuture().GetValueSync();
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            operation->Watch().GetValueSync(),
            TOperationFailedError,
            "Operation is running for too long");
    }
}

TEST(Operations, FuturesOfAttachedOperation)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
        writer->AddRow(TNode()("foo", "baz"));
        writer->Finish();
    }

    TOperationId operationId;

    {
        auto operation = client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TSleepingMapper(TDuration::Seconds(100)),
            TOperationOptions().Wait(false));
        operationId = operation->GetId();
    }

    auto attached = client->AttachOperation(operationId);
    attached->GetPreparedFuture().GetValueSync();
    attached->GetStartedFuture().GetValueSync();

    attached->AbortOperation();
    EXPECT_THROW(attached->Watch().GetValueSync(), TOperationFailedError);
}

TEST(Operations, StartOperationModes)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("key", 1));
        writer->Finish();
    }

    for (auto mode : {
        TOperationOptions::EStartOperationMode::AsyncPrepare,
        TOperationOptions::EStartOperationMode::AsyncStart,
        TOperationOptions::EStartOperationMode::SyncStart,
        TOperationOptions::EStartOperationMode::SyncWait,
    }) {
        auto operation = client->Map(
            new TIdMapper,
            {inputTable},
            {workingDir + "/output"},
            TMapOperationSpec(),
            TOperationOptions().StartOperationMode(mode));
        operation->GetPreparedFuture().GetValueSync();
        if (mode == TOperationOptions::EStartOperationMode::AsyncPrepare) {
            operation->Start();
        }
        operation->GetStartedFuture().GetValueSync();
        operation->Watch().GetValueSync();
    }
}

TEST(Operations, GetStatus)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->StartOperationRetryCount = 5;
    TConfig::Get()->StartOperationRetryInterval = TDuration::Seconds(1);

    CreateTableWithFooColumn(client, workingDir + "/input");

    auto outage = TAbortableHttpResponse::StartOutage("/start_op");
    auto operation = client->Map(
        new TIdMapper,
        {workingDir + "/input"},
        {workingDir + "/output"},
        TMapOperationSpec(),
        TOperationOptions().StartOperationMode(TOperationOptions::EStartOperationMode::AsyncStart));

    while (!operation->GetStatus().StartsWith("Retriable error during operation start")) {
        Sleep(TDuration::MilliSeconds(100));
    }
    outage.Stop();
    operation->Watch().GetValueSync();
    EXPECT_EQ(operation->GetStatus(), "On YT cluster: completed");
}

TEST(Operations, MiscOptions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto inputTable = TRichYPath(workingDir + "/input");
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("key", 1));
        writer->Finish();
    }

    auto operation = client->Map(
        new TIdMapper,
        {inputTable},
        {workingDir + "/output"},
        TMapOperationSpec()
        .Weight(2.0)
        .PoolTrees({"default"})
            .ResourceLimits(
                TSchedulerResources()
                    .UserSlots(42)
                    .Cpu(54)
                    .Memory(500_MB)
                )
        );

    auto spec = *operation->GetAttributes().Spec;
    EXPECT_EQ(spec["resource_limits"]["user_slots"].AsInt64(), 42);
    EXPECT_EQ(spec["resource_limits"]["cpu"].AsInt64(), 54);
    EXPECT_EQ(spec["resource_limits"]["memory"].AsInt64(), static_cast<i64>(500_MB));
    EXPECT_EQ(spec["resource_limits"]["memory"].AsInt64(), static_cast<i64>(500_MB));
    EXPECT_EQ(spec["pool_trees"], TNode::CreateList().Add("default"));
    EXPECT_EQ(spec["weight"].AsDouble(), 2.0);
}
