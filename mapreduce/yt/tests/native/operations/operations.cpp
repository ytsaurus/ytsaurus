#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/tests/native/proto_lib/all_types.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/all_types_proto3.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/row.pb.h>

#include <mapreduce/yt/tests/native/ydl_lib/row.ydl.h>
#include <mapreduce/yt/tests/native/ydl_lib/all_types.ydl.h>

#include <mapreduce/yt/interface/logging/logger.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/debug_metrics.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/finally_guard.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <mapreduce/yt/library/lazy_sort/lazy_sort.h>
#include <mapreduce/yt/library/operation_tracker/operation_tracker.h>

#include <mapreduce/yt/raw_client/raw_requests.h>

#include <mapreduce/yt/util/wait_for_tablets_state.h>

#include <library/cpp/digest/md5/md5.h>

#include <library/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/scope.h>
#include <util/generic/xrange.h>
#include <util/folder/path.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/mktemp.h>
#include <util/system/tempfile.h>
#include <util/thread/factory.h>

using namespace NYT;
using namespace NYT::NTesting;

namespace NYdlRows = mapreduce::yt::tests::native::ydl_lib::row;
namespace NYdlAllTypes = mapreduce::yt::tests::native::ydl_lib::all_types;

////////////////////////////////////////////////////////////////////////////////

static void WaitOperationPredicate(
    const IOperationPtr& operation,
    const std::function<bool(const TOperationAttributes&)>& predicate,
    const TString& failMsg = "")
{
    try {
        WaitForPredicate([&] {
            return predicate(operation->GetAttributes());
        });
    } catch (const TWaitFailedException& exception) {
        ythrow yexception() << "Wait for operation " << operation->GetId() << " failed: "
            << failMsg << ".\n" << exception.what();
    }
}

static void WaitOperationHasState(const IOperationPtr& operation, const TString& state)
{
    WaitOperationPredicate(
        operation,
        [&] (const TOperationAttributes& attrs) {
            return attrs.State == state;
        },
        "state should become \"" + state + "\"");
}

static void WaitOperationIsRunning(const IOperationPtr& operation)
{
    WaitOperationHasState(operation, "running");
}

static void WaitOperationHasBriefProgress(const IOperationPtr& operation)
{
    WaitOperationPredicate(
        operation,
        [&] (const TOperationAttributes& attributes) {
            return attributes.BriefProgress.Defined();
        },
        "brief progress should have become available");
}

static TString GetOperationState(const IClientPtr& client, const TOperationId& operationId)
{
    const auto& state = client->GetOperation(operationId).State;
    UNIT_ASSERT(state.Defined());
    return *state;
}

static void EmulateOperationArchivation(IClientPtr& client, const TOperationId& operationId)
{
    auto idStr = GetGuidAsString(operationId);
    auto lastTwoDigits = idStr.substr(idStr.size() - 2, 2);
    TString path = TStringBuilder() << "//sys/operations/" << lastTwoDigits << "/" << idStr;
    client->Remove(path, TRemoveOptions().Recursive(true));
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

class TRangeBasedTIdMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (const auto& cursor : *reader) {
            writer->AddRow(cursor.GetRow());
        }
    }
};
REGISTER_MAPPER(TRangeBasedTIdMapper);

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

class TUrlRowIdMapper : public IMapper<TTableReader<TUrlRow>, TTableWriter<TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};
REGISTER_MAPPER(TUrlRowIdMapper);

////////////////////////////////////////////////////////////////////////////////

class TYdlUrlRowIdMapper : public IMapper<TTableReader<NYdlRows::TUrlRow>, TTableWriter<NYdlRows::TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};
REGISTER_MAPPER(TYdlUrlRowIdMapper);

////////////////////////////////////////////////////////////////////////////////

class TUrlRowIdReducer : public IReducer<TTableReader<TUrlRow>, TTableWriter<TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};
REGISTER_REDUCER(TUrlRowIdReducer);

////////////////////////////////////////////////////////////////////////////////

class TYdlUrlRowIdReducer : public IReducer<TTableReader<NYdlRows::TUrlRow>, TTableWriter<NYdlRows::TUrlRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};
REGISTER_REDUCER(TYdlUrlRowIdReducer);

////////////////////////////////////////////////////////////////////////////////

class TYdlMultipleInputMapper
    : public IMapper<TTableReader<TYdlOneOf<NYdlRows::TUrlRow, NYdlRows::THostRow>>, TTableWriter<NYdlRows::TRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            NYdlRows::TRow row;
            if (reader->GetTableIndex() == 0) {
                row.SetStringField(*reader->GetRow<NYdlRows::TUrlRow>().GetHost());
            } else if (reader->GetTableIndex() == 1) {
                row.SetStringField(*reader->GetRow<NYdlRows::THostRow>().GetHost());
            }
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TYdlMultipleInputMapper);

////////////////////////////////////////////////////////////////////////////////

class TYdlFailingInputMapper
    : public IMapper<TTableReader<TYdlOneOf<NYdlRows::TUrlRow, NYdlRows::THostRow>>, TTableWriter<NYdlRows::TRow>>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            NYdlRows::TRow row;
            if (reader->GetTableIndex() == 0) {
                row.SetStringField(*reader->GetRow<NYdlRows::TUrlRow>().GetHost());
            } else if (reader->GetTableIndex() == 1) {
                row.SetStringField(*reader->GetRow<NYdlRows::TUrlRow>().GetHost());
            }
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TYdlFailingInputMapper);

////////////////////////////////////////////////////////////////////////////////

class TYdlFailingOutputMapper
    : public IMapper<TTableReader<NYdlRows::TUrlRow>, TYdlTableWriter>
{
public:
    void Do(TReader* reader, TWriter* writer)
    {
        for (; reader->IsValid(); reader->Next()) {
            NYdlRows::TRow row;
            row.SetStringField(*reader->GetRow().GetHost());
            writer->AddRow<NYdlRows::TRow>(row, 1);
        }
    }
};
REGISTER_MAPPER(TYdlFailingOutputMapper);

////////////////////////////////////////////////////////////////////////////////

class TAlwaysFailingMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter*)
    {
        for (; reader->IsValid(); reader->Next()) {
        }
        Cerr << "This mapper always fails" << Endl;
        ::exit(1);
    }
};
REGISTER_MAPPER(TAlwaysFailingMapper);

////////////////////////////////////////////////////////////////////////////////


class TMapperThatWritesStderr : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TMapperThatWritesStderr() = default;

    TMapperThatWritesStderr(TStringBuf str)
        : Stderr_(str)
    { }

    void Do(TReader* reader, TWriter*) override {
        for (; reader->IsValid(); reader->Next()) {
        }
        Cerr << Stderr_;
    }

    Y_SAVELOAD_JOB(Stderr_);

private:
    TString Stderr_;
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

class TWriteFileThenSleepMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TWriteFileThenSleepMapper() = default;

    TWriteFileThenSleepMapper(TString fileName, TDuration sleepDuration)
        : FileName_(std::move(fileName))
        , SleepDuration_(sleepDuration)
    { }

    virtual void Do(TReader*, TWriter* ) override
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

class TProtobufMapperProto3 : public IMapper<TTableReader<TAllTypesMessageProto3>, TTableWriter<TAllTypesMessageProto3>>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        TAllTypesMessageProto3 row;
        for (; reader->IsValid(); reader->Next()) {
            reader->MoveRow(&row);
            row.SetStringField(row.GetStringField() + " mapped");
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TProtobufMapperProto3);

////////////////////////////////////////////////////////////////////////////////

class TComplexTypesProtobufMapper
    : public IMapper<TTableReader<Message>, TTableWriter<Message>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            if (reader->GetTableIndex() == 0) {
                auto row = reader->MoveRow<TRowMixedSerializationOptions>();
                row.MutableUrlRow_1()->SetHost(row.GetUrlRow_1().GetHost() + ".mapped");
                row.MutableUrlRow_2()->SetHost(row.GetUrlRow_2().GetHost() + ".mapped");
                writer->AddRow(row, 0);
            } else {
                Y_ENSURE(reader->GetTableIndex() == 1);
                auto row = reader->MoveRow<TRowSerializedRepeatedFields>();
                row.AddInts(40000);
                writer->AddRow(row, 1);
            }
        }
    }
};
REGISTER_MAPPER(TComplexTypesProtobufMapper);

////////////////////////////////////////////////////////////////////////////////

class TProtobufMapperTypeOptions
    : public IMapper<TTableReader<Message>, TTableWriter<Message>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->MoveRow<TRowWithTypeOptions>();
            auto any = NodeFromYsonString(row.GetAnyField());
            any["new"] = "delete";
            row.SetAnyField(NodeToYsonString(any));
            row.AddRepeatedEnumIntField(TRowWithTypeOptions::BLUE);
            auto otherColumns = NodeFromYsonString(row.GetOtherColumnsField());
            otherColumns["NewColumn"] = "BrandNew";
            row.SetOtherColumnsField(NodeToYsonString(otherColumns));
            writer->AddRow(row);
        }
    }
};
REGISTER_MAPPER(TProtobufMapperTypeOptions);

////////////////////////////////////////////////////////////////////////////////

class TYdlMapper : public IMapper<TTableReader<NYdlRows::TRow>, TYdlTableWriter>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        NYdlRows::TRow row;
        for (; reader->IsValid(); reader->Next()) {
            reader->MoveRow(&row);
            row.SetStringField(row.GetStringField() + " mapped");
            writer->AddRow<NYdlRows::TRow>(row);
        }
    }
};
REGISTER_MAPPER(TYdlMapper);

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

class TSplitGoodUrlYdlMapper : public IMapper<TTableReader<NYdlRows::TUrlRow>, TYdlTableWriter>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto urlRow = reader->GetRow();
            if (urlRow.GetHttpCode() == 200) {
                NYdlRows::TGoodUrl goodUrl;
                goodUrl.SetUrl(*urlRow.GetHost() + *urlRow.GetPath());
                writer->AddRow<NYdlRows::TGoodUrl>(goodUrl, 1);
            }
            writer->AddRow<NYdlRows::TUrlRow>(urlRow, 0);
        }
    }
};
REGISTER_MAPPER(TSplitGoodUrlYdlMapper);

////////////////////////////////////////////////////////////////////////////////

class TCountHttpCodeTotalYdlReducer : public IReducer<TTableReader<NYdlRows::TUrlRow>, TTableWriter<NYdlRows::THostRow>>
{
public:
    virtual void Do(TReader* reader, TWriter* writer) override
    {
        NYdlRows::THostRow hostRow;
        i32 total = 0;
        for (; reader->IsValid(); reader->Next()) {
            auto urlRow = reader->GetRow();
            if (hostRow.GetHost().Empty()) {
                hostRow.SetHost(urlRow.GetHost());
            }
            total += *urlRow.GetHttpCode();
        }
        hostRow.SetHttpCodeTotal(total);
        writer->AddRow(hostRow);
    }
};
REGISTER_REDUCER(TCountHttpCodeTotalYdlReducer);

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
REGISTER_VANILLA_JOB(TVanillaAppendingToFile);

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
REGISTER_VANILLA_JOB(TVanillaWithTableOutput);

////////////////////////////////////////////////////////////////////////////////

class TFailingVanilla : public IVanillaJob<>
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
REGISTER_VANILLA_JOB(TVanillaWithPorts);

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

template <class TInputRowTypeType>
class TMapperThatWritesRowsAndRanges : public ::IMapper<TTableReader<TInputRowTypeType>, TNodeWriter>
{
public:
    using TReader = TTableReader<TInputRowTypeType>;
    using TWriter = TNodeWriter;
    void Do(TReader* reader, TWriter* writer) override {
        for (; reader->IsValid(); reader->Next()) {
            auto row = TNode()
                ("row_id", reader->GetRowIndex())
                ("range_id", reader->GetRangeIndex());
            writer->AddRow(row);
        }
    }
};

REGISTER_MAPPER(TMapperThatWritesRowsAndRanges<TNode>);
REGISTER_MAPPER(TMapperThatWritesRowsAndRanges<TYaMRRow>);
REGISTER_MAPPER(TMapperThatWritesRowsAndRanges<TEmbeddedMessage>);
REGISTER_MAPPER(TMapperThatWritesRowsAndRanges<NYdlRows::TMessage>);

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

class TReducerThatCountsOutputTables : public IReducer<TNodeReader, TNodeWriter>
{
public:
    TReducerThatCountsOutputTables() = default;

    void Do(TReader*, TWriter* writer) override
    {
        writer->AddRow(TNode()("result", GetOutputTableCount()), 0);
    }
};
REGISTER_REDUCER(TReducerThatCountsOutputTables);

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

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(Operations)
{
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

        auto reader = client->CreateTableReader<TNode>(workingDir + "/output");
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("NewKey", "key")("Value", "value"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(RenameColumns_Yson)
    {
        TestRenameColumns(ENodeReaderFormat::Yson);
    }

    Y_UNIT_TEST(RenameColumns_Skiff)
    {
        TestRenameColumns(ENodeReaderFormat::Skiff);
    }

    Y_UNIT_TEST(IncorrectTableId)
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

    Y_UNIT_TEST(EnableKeyGuarantee)
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
        UNIT_ASSERT_EQUAL((*spec)["enable_key_guarantee"].AsBool(), false);
    }

    Y_UNIT_TEST(OrderedMapReduce)
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
        UNIT_ASSERT_EQUAL((*spec)["ordered"].AsBool(), true);
    }

    Y_UNIT_TEST(CustomTitle)
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
            UNIT_ASSERT_EQUAL((*spec)["title"].AsString(), newTitle);
        }

        {
            auto op = client->Map(
                TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input")
                .AddOutput<TNode>(workingDir + "/output"),
                new TIdMapper);
            auto spec = client->GetOperation(op->GetId()).Spec;
            UNIT_ASSERT_STRING_CONTAINS((*spec)["title"].AsString(), "TIdMapper");
        }
    }

    Y_UNIT_TEST(RangeBasedReader)
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
        auto reader = client->CreateTableReader<TNode>(outputTable);

        TVector<TNode> result;
        for (const auto& cursor : *reader) {
            result.push_back(cursor.GetRow());
        }
        UNIT_ASSERT_VALUES_EQUAL(data, result);
    }

    Y_UNIT_TEST(RangeBasedReaderEmptyTable)
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
        auto reader = client->CreateTableReader<TNode>(outputTable);

        TVector<TNode> result;
        for (const auto& cursor : *reader) {
            result.push_back(cursor.GetRow());
        }
        UNIT_ASSERT(result.empty());
    }

    Y_UNIT_TEST(MaxFailedJobCount)
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
            UNIT_ASSERT_EXCEPTION(future.GetValue(), TOperationFailedError);

            WaitOperationHasBriefProgress(operation);

            const auto& briefProgress = operation->GetBriefProgress();
            UNIT_ASSERT(briefProgress);
            UNIT_ASSERT_VALUES_EQUAL(briefProgress->Failed, maxFailedJobCount);
        }
    }

    Y_UNIT_TEST(FailOnJobRestart)
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
            UNIT_FAIL("Operation expected to fail");
        } catch (const TOperationFailedError& e) {
            operationId = e.GetOperationId();
        }

        const auto& briefProgress = client->GetOperation(operationId).BriefProgress;
        UNIT_ASSERT(briefProgress);
        UNIT_ASSERT_VALUES_EQUAL(briefProgress->Failed, 1);
    }

    Y_UNIT_TEST(StderrTablePath)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto expectedStderr = AsStringBuf("PYSHCH");
        client->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output")
            .StderrTablePath(workingDir + "/stderr"),
            new TMapperThatWritesStderr(expectedStderr));

        auto reader = client->CreateTableReader<TNode>(workingDir + "/stderr");
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT(reader->GetRow()["data"].AsString().Contains(expectedStderr));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(CreateDebugOutputTables)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto expectedStderr = AsStringBuf("PYSHCH");

        // stderr table does not exist => should fail
        UNIT_ASSERT_EXCEPTION(
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
        UNIT_ASSERT_NO_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(workingDir + "/input")
                    .AddOutput<TNode>(workingDir + "/output")
                    .StderrTablePath(workingDir + "/stderr"),
                new TMapperThatWritesStderr(expectedStderr),
                TOperationOptions()
                    .CreateDebugOutputTables(false)));
    }

    Y_UNIT_TEST(CreateOutputTables)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/input");
            writer->AddRow(TNode()("foo", "bar"));
            writer->Finish();
        }

        auto expectedStderr = AsStringBuf("PYSHCH");

        // Output table does not exist => operation should fail.
        UNIT_ASSERT_EXCEPTION(
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
        UNIT_ASSERT_NO_EXCEPTION(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(workingDir + "/input")
                    .AddOutput<TNode>(workingDir + "/output")
                    .StderrTablePath(workingDir + "/stderr"),
                new TMapperThatWritesStderr(expectedStderr),
                TOperationOptions()
                    .CreateOutputTables(false)));

        // Inputs not checked => we get TApiUsageError.
        UNIT_ASSERT_EXCEPTION(
            client->Sort(
                TSortOperationSpec()
                    .AddInput(workingDir + "/nonexistent-input")
                    .Output(workingDir + "/nonexistent-input")),
            TApiUsageError);

        // Inputs are not checked => we get an error response from the server.
        UNIT_ASSERT_EXCEPTION(
            client->Sort(
                TSortOperationSpec()
                    .AddInput(workingDir + "/nonexistent-input")
                    .Output(workingDir + "/nonexistent-input"),
                TOperationOptions()
                    .CreateOutputTables(false)),
            TOperationFailedError);
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

    Y_UNIT_TEST(TestFetchTable)
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

    Y_UNIT_TEST(TestFetchTableRange)
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

    Y_UNIT_TEST(TestReadProtobufFileInJob)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT(operation->GetError().Empty());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT(operation->GetError().Empty());
    }

    Y_UNIT_TEST(TestGetOperationStatus_Failed)
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
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Failed);
        UNIT_ASSERT(operation->GetError().Defined());

        EmulateOperationArchivation(client, operation->GetId());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Failed);
        UNIT_ASSERT(operation->GetError().Defined());
    }

    Y_UNIT_TEST(TestGetOperationStatistics)
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
        UNIT_ASSERT(jobStatistics.GetStatistics("time/total").Max().Defined());
    }

    Y_UNIT_TEST(TestCustomStatistics)
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

    void MapWithProtobuf(bool useDeprecatedAddInput, bool useClientProtobuf)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->UseClientProtobuf = useClientProtobuf;

        auto inputTable = TRichYPath(workingDir + "/input");
        auto outputTable = TRichYPath(workingDir + "/output");
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

    Y_UNIT_TEST(ProtobufMapProto3)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable = TRichYPath(workingDir + "/input");
        auto outputTable = TRichYPath(workingDir + "/output");
        {
            auto writer = client->CreateTableWriter<TNode>(inputTable);
            writer->AddRow(TNode()("StringField", "raz"));
            writer->AddRow(TNode()("StringField", "dva"));
            writer->AddRow(TNode()("StringField", "tri"));
            writer->Finish();
        }
        auto spec = TMapOperationSpec()
            .AddInput<TAllTypesMessageProto3>(inputTable)
            .AddOutput<TAllTypesMessageProto3>(outputTable);

        client->Map(spec, new TProtobufMapperProto3);

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

    Y_UNIT_TEST(ProtobufMap_ComplexTypes)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto urlRowRawTypeV2 = TNode()
            ("metatype", "struct")
            ("fields", TNode()
                .Add(TNode()("name", "Host")("type", "string"))
                .Add(TNode()("name", "Path")("type", "string"))
                .Add(TNode()("name", "HttpCode")("type", "int32")));

        auto schema1 = TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").RawTypeV2(urlRowRawTypeV2))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(VT_STRING));

        auto schema2 = TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("Ints")
                .RawTypeV2(TNode()
                    ("metatype", "list")
                    ("element", "int64")))
            .AddColumn(TColumnSchema()
                .Name("UrlRows")
                .RawTypeV2(TNode()
                    ("metatype", "list")
                    ("element", urlRowRawTypeV2)));

        auto inputTable1 = TRichYPath(workingDir + "/input_1").Schema(schema1);
        auto inputTable2 = TRichYPath(workingDir + "/input_2").Schema(schema2);
        auto outputTable1 = TRichYPath(workingDir + "/output_1").Schema(schema1);
        auto outputTable2 = TRichYPath(workingDir + "/output_2").Schema(schema2);

        {
            // TRowMixedSerializationOptions.
            // UrlRow_2 has the same value as UrlRow_1.
            auto writer = client->CreateTableWriter<TNode>(inputTable1);
            writer->AddRow(TNode()
                ("UrlRow_1", TNode()("Host", "ya.ru")("Path", "/mail")("HttpCode", 404))
                ("UrlRow_2",
                     "\x0A" "\x05" "\x79\x61\x2E\x72\x75"
                     "\x12" "\x05" "\x2F\x6D\x61\x69\x6C"
                     "\x18" "\xA8\x06"));
            writer->AddRow(TNode()
                ("UrlRow_1", TNode()("Host", "ya.ru")("Path", "/maps")("HttpCode", 300))
                ("UrlRow_2",
                    "\x0A" "\x05" "\x79\x61\x2E\x72\x75"
                    "\x12" "\x05" "\x2F\x6D\x61\x70\x73"
                    "\x18" "\xD8\x04"));
            writer->Finish();
        }

        {
            // TRowSerializedRepeatedFields.
            auto writer = client->CreateTableWriter<TNode>(inputTable2);
            writer->AddRow(TNode()
                ("Ints", TNode().Add(-1).Add(-2))
                ("UrlRows", TNode()
                    .Add(TNode()("Host", "yandex.ru")("Path", "/mail")("HttpCode", 200))
                    .Add(TNode()("Host", "google.com")("Path", "/mail")("HttpCode", 404))));
            writer->AddRow(TNode()
                ("Ints", TNode().Add(1).Add(2))
                ("UrlRows", TNode()
                    .Add(TNode()("Host", "yandex.ru")("Path", "/maps")("HttpCode", 200))
                    .Add(TNode()("Host", "google.com")("Path", "/maps")("HttpCode", 404))));
            writer->Finish();
        }

        client->Map(
            TMapOperationSpec()
                .AddInput<TRowMixedSerializationOptions>(inputTable1)
                .AddInput<TRowSerializedRepeatedFields>(inputTable2)
                .AddOutput<TRowMixedSerializationOptions>(outputTable1)
                .AddOutput<TRowSerializedRepeatedFields>(outputTable2),
            new TComplexTypesProtobufMapper);

        TVector<TNode> expectedContent1 = {
            TNode()
                ("UrlRow_1", TNode()("Host", "ya.ru.mapped")("Path", "/mail")("HttpCode", 404))
                ("UrlRow_2",
                    "\x0A" "\x0C" "\x79\x61\x2E\x72\x75\x2E\x6D\x61\x70\x70\x65\x64"
                    "\x12" "\x05" "\x2F\x6D\x61\x69\x6C"
                    "\x18" "\xA8\x06"),
            TNode()
                ("UrlRow_1", TNode()("Host", "ya.ru.mapped")("Path", "/maps")("HttpCode", 300))
                ("UrlRow_2",
                    "\x0A" "\x0C" "\x79\x61\x2E\x72\x75\x2E\x6D\x61\x70\x70\x65\x64"
                    "\x12" "\x05" "\x2F\x6D\x61\x70\x73"
                    "\x18" "\xD8\x04"),
        };

        TVector<TNode> expectedContent2 = {
            TNode()
                ("Ints", TNode().Add(-1).Add(-2).Add(40000))
                ("UrlRows", TNode()
                    .Add(TNode()("Host", "yandex.ru")("Path", "/mail")("HttpCode", 200))
                    .Add(TNode()("Host", "google.com")("Path", "/mail")("HttpCode", 404))),
            TNode()
                ("Ints", TNode().Add(1).Add(2).Add(40000))
                ("UrlRows", TNode()
                    .Add(TNode()("Host", "yandex.ru")("Path", "/maps")("HttpCode", 200))
                    .Add(TNode()("Host", "google.com")("Path", "/maps")("HttpCode", 404)))
        };

        auto actualContent1 = ReadTable(client, outputTable1.Path_);
        auto actualContent2 = ReadTable(client, outputTable2.Path_);

        UNIT_ASSERT_VALUES_EQUAL(expectedContent1, actualContent1);
        UNIT_ASSERT_VALUES_EQUAL(expectedContent2, actualContent2);
    }

    Y_UNIT_TEST(ProtobufMap_TypeOptions)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto schema = TTableSchema()
            .Strict(false)
            .AddColumn(TColumnSchema()
                .Name("ColorIntField").Type(EValueType::VT_INT64))
            .AddColumn(TColumnSchema()
                .Name("ColorStringField").Type(EValueType::VT_STRING))
            .AddColumn(TColumnSchema()
                .Name("AnyField").Type(EValueType::VT_ANY))
            .AddColumn(TColumnSchema()
                .Name("EmbeddedField").RawTypeV2(TNode()
                    ("metatype", "optional")
                    ("element", TNode()
                        ("metatype", "struct")
                        ("fields", TNode()
                            .Add(TNode()
                                ("name", "ColorIntField")
                                ("type", "int64"))
                            .Add(TNode()
                                ("name", "ColorStringField")
                                ("type", "string"))
                            .Add(TNode()
                                ("name", "AnyField")
                                ("type", TNode()
                                    ("metatype", "optional")
                                    ("element", "any")))))))
            .AddColumn(TColumnSchema()
                .Name("RepeatedEnumIntField").RawTypeV2(TNode()
                    ("metatype", "list")
                    ("element", "int64")))
            .AddColumn(TColumnSchema()
                .Name("UnknownSchematizedColumn").Type(EValueType::VT_BOOLEAN));

        auto inputTable = TRichYPath(workingDir + "/input").Schema(schema);
        auto outputTable = TRichYPath(workingDir + "/output").Schema(schema);

        {
            auto writer = client->CreateTableWriter<TNode>(inputTable);
            writer->AddRow(TNode()
                ("ColorIntField", -1)
                ("ColorStringField", "BLUE")
                ("AnyField", TNode()("x", TNode()("y", 12)))
                ("UnknownSchematizedColumn", true)
                ("UnknownUnschematizedColumn", 1234)
                ("EmbeddedField", TNode()
                    ("ColorIntField", 0)
                    ("ColorStringField", "RED")
                    ("AnyField", TNode()("key", "value")))
                ("RepeatedEnumIntField", TNode().Add(0).Add(1).Add(-1)));
            writer->AddRow(TNode()
                ("ColorIntField", 0)
                ("ColorStringField", "RED")
                ("AnyField", TNode()("z", 0))
                ("UnknownSchematizedColumn", false)
                ("UnknownUnschematizedColumn", "some-string")
                ("EmbeddedField", TNode()
                    ("ColorIntField", -1)
                    ("ColorStringField", "WHITE")
                    ("AnyField", "hooray"))
                ("RepeatedEnumIntField", TNode().Add(1)));
            writer->Finish();
        }

        client->Map(
            TMapOperationSpec()
                .AddInput<TRowWithTypeOptions>(inputTable)
                .AddOutput<TRowWithTypeOptions>(outputTable),
            new TProtobufMapperTypeOptions);

        auto actualRows = ReadTable(client, outputTable.Path_);
        UNIT_ASSERT_VALUES_EQUAL(actualRows.size(), 2);
        {
            const auto& row = actualRows[0];
            UNIT_ASSERT_VALUES_EQUAL(row["ColorIntField"], -1);
            UNIT_ASSERT_VALUES_EQUAL(row["ColorStringField"], "BLUE");
            UNIT_ASSERT_VALUES_EQUAL(
                row["AnyField"],
                TNode()
                    ("x", TNode()("y", 12))
                    ("new", "delete"));
            UNIT_ASSERT_VALUES_EQUAL(row["UnknownSchematizedColumn"], true);
            UNIT_ASSERT_VALUES_EQUAL(row["UnknownUnschematizedColumn"], 1234);
            UNIT_ASSERT_VALUES_EQUAL(
                row["EmbeddedField"],
                TNode()
                    ("ColorIntField", 0)
                    ("ColorStringField", "RED")
                    ("AnyField", TNode()("key", "value")));
            UNIT_ASSERT_VALUES_EQUAL(row["RepeatedEnumIntField"], TNode().Add(0).Add(1).Add(-1).Add(1));
            UNIT_ASSERT_VALUES_EQUAL(row["NewColumn"], "BrandNew");
        }
        {
            const auto& row = actualRows[1];
            UNIT_ASSERT_VALUES_EQUAL(row["ColorIntField"], 0);
            UNIT_ASSERT_VALUES_EQUAL(row["ColorStringField"], "RED");
            UNIT_ASSERT_VALUES_EQUAL(
                row["AnyField"],
                TNode()
                    ("z", 0)
                    ("new", "delete"));
            UNIT_ASSERT_VALUES_EQUAL(row["UnknownSchematizedColumn"], false);
            UNIT_ASSERT_VALUES_EQUAL(row["UnknownUnschematizedColumn"], "some-string");
            UNIT_ASSERT_VALUES_EQUAL(row["EmbeddedField"], TNode()
                ("ColorIntField", -1)
                ("ColorStringField", "WHITE")
                ("AnyField", "hooray"));
            UNIT_ASSERT_VALUES_EQUAL(row["RepeatedEnumIntField"], TNode().Add(1).Add(1));
        }
    }
    Y_UNIT_TEST(YdlMap)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable = TRichYPath(workingDir + "/input");
        auto outputTable = TRichYPath(workingDir + "/output");
        {
            auto writer = client->CreateTableWriter<TNode>(inputTable);
            writer->AddRow(TNode()("StringField", "raz"));
            writer->AddRow(TNode()("StringField", "dva"));
            writer->AddRow(TNode()("StringField", "tri"));
            writer->Finish();
        }

        client->Map(
            new TYdlMapper,
            Structured<NYdlRows::TRow>(inputTable),
            Structured<NYdlRows::TRow>(outputTable));

        TVector<TNode> expected = {
            TNode()("StringField", "raz mapped"),
            TNode()("StringField", "dva mapped"),
            TNode()("StringField", "tri mapped"),
        };
        auto actual = ReadTable(client, outputTable.Path_);
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(MultipleInputYdlMap)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable1 = TRichYPath(workingDir + "/input1");
        auto inputTable2 = TRichYPath(workingDir + "/input2");
        auto outputTable = TRichYPath(workingDir + "/output");

        {
            auto writer = client->CreateTableWriter<NYdlRows::TUrlRow>(inputTable1);
            NYdlRows::TUrlRow row;
            row.SetHost("https://www.google.com");
            writer->AddRow(row);
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<NYdlRows::THostRow>(inputTable2);
            NYdlRows::THostRow row;
            row.SetHost("https://www.yandex.ru");
            writer->AddRow(row);
            writer->Finish();
        }

        client->Map(
            new TYdlMultipleInputMapper,
            {Structured<NYdlRows::TUrlRow>(inputTable1), Structured<NYdlRows::THostRow>(inputTable2)},
            Structured<NYdlRows::TRow>(outputTable));

        client->Sort(outputTable, outputTable, "StringField");

        TVector<TNode> expected = {
            TNode()("StringField", "https://www.google.com"),
            TNode()("StringField", "https://www.yandex.ru"),
        };
        auto actual = ReadTable(client, outputTable.Path_);
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(YdlRowTypeCheckFail)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable1 = TRichYPath(workingDir + "/input1");
        auto inputTable2 = TRichYPath(workingDir + "/input2");
        auto outputTable1 = TRichYPath(workingDir + "/output1");
        auto outputTable2 = TRichYPath(workingDir + "/output2");

        {
            auto writer = client->CreateTableWriter<NYdlRows::TUrlRow>(inputTable1);
            NYdlRows::TUrlRow row;
            row.SetHost("https://www.google.com");
            writer->AddRow(row);
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<NYdlRows::THostRow>(inputTable2);
            NYdlRows::THostRow row;
            row.SetHost("https://www.yandex.ru");
            writer->AddRow(row);
            writer->Finish();
        }

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            client->Map(
                new TYdlFailingInputMapper,
               {Structured<NYdlRows::TUrlRow>(inputTable1), Structured<NYdlRows::THostRow>(inputTable2)},
                Structured<NYdlRows::TRow>(outputTable1)),
            TOperationFailedError,
            "Invalid row type at index");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            client->Map(
                new TYdlFailingOutputMapper,
                Structured<NYdlRows::TUrlRow>(inputTable1),
                {Structured<NYdlRows::TRow>(outputTable1), Structured<NYdlRows::THostRow>(outputTable2)}),
            TOperationFailedError,
            "Invalid row type at index");
    }

    Y_UNIT_TEST(JobPrefix)
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

    Y_UNIT_TEST(JobEnvironment)
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
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRow()["TEST_ENV"], "foo bar baz");
        }
    }

    Y_UNIT_TEST(MapReduceMapOutput)
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

        UNIT_ASSERT_VALUES_EQUAL(
            ReadTable(client, workingDir + "/output"),
            TVector<TNode>{TNode()("key", "foo")("value", "bar")});

        UNIT_ASSERT_VALUES_EQUAL(
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
            row.SetHost("http://example.com");
            row.SetPath("/index.php");
            row.SetHttpCode(200);
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

        UNIT_ASSERT_VALUES_EQUAL(
            ReadTable(client, workingDir + "/output"),
            TVector<TNode>{TNode()("Host", "http://example.com")("HttpCodeTotal", 200)});

        UNIT_ASSERT_VALUES_EQUAL(
            ReadTable(client, workingDir + "/map_output"),
            TVector<TNode>{TNode()("Url", "http://example.com/index.php")});
    }

    Y_UNIT_TEST(MapReduceMapOutputProtobuf)
    {
        TestMapReduceMapOutput<
           TUrlRow,
           TGoodUrl,
           THostRow,
           TSplitGoodUrlMapper,
           TCountHttpCodeTotalReducer>();
    }

    Y_UNIT_TEST(MapReduceMapOutputYdl)
    {
        TestMapReduceMapOutput<
            NYdlRows::TUrlRow,
            NYdlRows::TGoodUrl,
            NYdlRows::THostRow,
            TSplitGoodUrlYdlMapper,
            TCountHttpCodeTotalYdlReducer>();
    }

    Y_UNIT_TEST(AddLocalFile)
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

    Y_UNIT_TEST(TestFailWithNoInputOutput)
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
            UNIT_ASSERT_EXCEPTION(client->Map(
                TMapOperationSpec()
                .AddInput<TNode>(workingDir + "/input"),
                new TIdMapper), TApiUsageError);
        }

        {
            UNIT_ASSERT_EXCEPTION(client->Map(
                TMapOperationSpec()
                .AddOutput<TNode>(workingDir + "/output"),
                new TIdMapper), TApiUsageError);
        }
    }

    Y_UNIT_TEST(MaxOperationCountExceeded)
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

        NYT::NDetail::TFinallyGuard finally([&]{
            for (auto& operation : operations) {
                operation->AbortOperation();
            }
        });

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
            UNIT_FAIL("Too many Maps must have been failed");
        } catch (const TErrorResponse& error) {
            // It's OK
        }
    }

    Y_UNIT_TEST(NetworkProblems)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->StartOperationRetryCount = 3;
        TConfig::Get()->StartOperationRetryInterval = TDuration::MilliSeconds(0);

        CreateTableWithFooColumn(client, workingDir + "/input");

        try {
            auto outage = TAbortableHttpResponse::StartOutage("/map");
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(workingDir + "/input")
                    .AddOutput<TNode>(workingDir + "/output_1"),
                new TIdMapper());
            UNIT_FAIL("Start operation must have been failed");
        } catch (const TAbortedForTestPurpose&) {
            // It's OK
        }
        {
            auto outage = TAbortableHttpResponse::StartOutage("/map", TConfig::Get()->StartOperationRetryCount - 1);
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
    Y_UNIT_TEST(JobNodeReader_Skiff_ComplexTypes)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto inTablePath = workingDir + "/in-table";
        auto outTablePath = workingDir + "/out-table";

        TTableSchema schema;
        schema.AddColumn("value", NTi::List(NTi::Int64()));

        client->Create(inTablePath, NT_TABLE, TCreateOptions().Attributes(TNode()("schema", schema.ToNode())));

        const auto row = TNode()("value", TNode::CreateList({TNode(1), TNode(2), TNode(3)}));
        {
            auto writer = client->CreateTableWriter<TNode>(inTablePath);
            writer->AddRow(row);
            writer->Finish();
        }
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Auto;
        client->Map(new TIdMapper(), inTablePath, outTablePath);
        auto reader = client->CreateTableReader<TNode>(outTablePath);
        std::vector<TNode> actualRows;
        for (const auto& cursor : *reader) {
            actualRows.push_back(cursor.GetRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(std::vector<TNode>{row}, actualRows);
    }

    Y_UNIT_TEST(TestSkiffOperationHint)
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
        auto reader = client->CreateTableReader<TNode>(workingDir + "/output");
        std::vector<TNode> actual;
        for (; reader->IsValid(); reader->Next()) {
            actual.push_back(reader->GetRow());
        }
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(TestSkiffOperationHintConfigurationConflict)
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

        UNIT_ASSERT_EXCEPTION(
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
            TVector<ui32> actualRangeIndecies;
            TVector<ui64> actualRowIndecies;
            for (; reader->IsValid(); reader->Next()) {
                auto row = reader->GetRow();
                actualRangeIndecies.push_back(row["range_id"].AsUint64());
                actualRowIndecies.push_back(row["row_id"].AsUint64());
            }
            const TVector<ui32> expectedRangeIndecies = {
                0, 0, 0, 0, 0,
                1, 1,
            };
            const TVector<ui64> expectedRowIndicies = {
                3, 4, 5, 6, 7,
                10, 11,
            };
            UNIT_ASSERT_VALUES_EQUAL(actualRangeIndecies, expectedRangeIndecies);
            UNIT_ASSERT_VALUES_EQUAL(actualRowIndecies, expectedRowIndicies);
        }
    }

    Y_UNIT_TEST(RangeIndices_Yson_TNode)
    {
        TestRangeIndices<TNode>(ENodeReaderFormat::Yson);
    }

    Y_UNIT_TEST(RangeIndices_Skiff_TNode)
    {
        TestRangeIndices<TNode>(ENodeReaderFormat::Skiff);
    }

    Y_UNIT_TEST(RangeIndices_TYaMRRow)
    {
        TestRangeIndices<TYaMRRow>(ENodeReaderFormat::Yson);
    }

    Y_UNIT_TEST(RangeIndices_Protobuf)
    {
        TestRangeIndices<TEmbeddedMessage>(ENodeReaderFormat::Yson);
    }

    Y_UNIT_TEST(RangeIndices_Ydl)
    {
        TestRangeIndices<NYdlRows::TMessage>(ENodeReaderFormat::Yson);
    }

    Y_UNIT_TEST(SkiffForInputQuery)
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

        UNIT_ASSERT_EXCEPTION(
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

        UNIT_ASSERT_NO_EXCEPTION(
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

    Y_UNIT_TEST(RetryLockConflict)
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

    Y_UNIT_TEST(Vanilla)
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
        UNIT_ASSERT_VALUES_EQUAL(stream.ReadAll().size(), (firstJobCount + secondJobCount) * message.size());
    }

    Y_UNIT_TEST(VanillaTableOutput)
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
        UNIT_ASSERT_VALUES_EQUAL(expected1, actual1);
        auto actual2 = ReadTable(client, outputTable2.Path_);
        UNIT_ASSERT_VALUES_EQUAL(expected2, actual2);
    }

    Y_UNIT_TEST(FailingVanilla)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TYPath stderrPath = workingDir + "/stderr";

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

    Y_UNIT_TEST(VanillaOutputTableCountCheck)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto outputTable = TRichYPath(workingDir + "/output");

        TTempFile tempFile(MakeTempName());
        Chmod(tempFile.Name().c_str(), 0777);

        TString message = "Hello world!";

        UNIT_ASSERT_EXCEPTION(
            client->RunVanilla(TVanillaOperationSpec()
                .AddTask(TVanillaTask()
                    .Job(new TVanillaWithTableOutput)
                    .JobCount(1)
                    .Name("vanilla"))),
            TApiUsageError);

        UNIT_ASSERT_EXCEPTION(
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

    Y_UNIT_TEST(LazySort)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        TString inputTable = workingDir + "/table";
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
            TString outputTable = workingDir + "/output";
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
            TString outputTable = workingDir + "/output";
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

        auto inputTables = TNode().Add(workingDir + "/input").AsList();
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
        UNIT_ASSERT_DOUBLES_EQUAL(client->Get(weightPath).AsDouble(), 10.0, 1e-9);
    }

    Y_UNIT_TEST(GetJob)
    {
        TTabletFixture tabletFixture;

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        auto expectedStderr = AsStringBuf("EXPECTED-STDERR");

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
            TString path = TStringBuilder()
                << "//sys/nodes/" << *job.Address
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

        auto expectedStderr = AsStringBuf("PYSHCH");
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


    Y_UNIT_TEST(FormatHint)
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
        const std::vector<TNode> expected = {TNode()("key", "foo")};
        auto readOutputAndRemove = [&] () {
            auto reader = client->CreateTableReader<TNode>(workingDir + "/output");
            std::vector<TNode> result;
            for (; reader->IsValid(); reader->Next()) {
                result.push_back(reader->GetRow());
            }
            client->Remove(workingDir + "/output");
            return result;
        };

        client->Map(
            TMapOperationSpec()
            .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdMapper);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->Reduce(
            TReduceOperationSpec()
            .InputFormatHints(TFormatHints().SkipNullValuesForTNode(true))
            .ReduceBy("key")
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdReducer);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->MapReduce(
            TMapReduceOperationSpec()
            .ReduceBy("key")
            .MapperFormatHints(TUserJobFormatHints().InputFormatHints(TFormatHints().SkipNullValuesForTNode(true)))
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdMapper,
            new TIdReducer);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->MapReduce(
            TMapReduceOperationSpec()
            .ReduceBy("key")
            .ReducerFormatHints(TUserJobFormatHints().InputFormatHints(TFormatHints().SkipNullValuesForTNode(true)))
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
            new TIdMapper,
            new TIdReducer);
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);
    }

    Y_UNIT_TEST(AttachOperation)
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

        UNIT_ASSERT_VALUES_EQUAL(operation->GetBriefState(), EOperationBriefState::Aborted);
    }

    Y_UNIT_TEST(AttachInexistingOperation)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        try {
            client->AttachOperation(GetGuid("1-2-3-4"));
            UNIT_FAIL("exception expected to be thrown");
        } catch (const TErrorResponse& e) {
            e.GetError().ContainsErrorCode(1915); // TODO: need named error code
        }
    }

    Y_UNIT_TEST(CrossTransactionMerge)
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
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    template<typename TRow, class TIdMapper, class TIdReducer>
    void TestSchemaInference(bool setOperationOptions)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TOperationOptions options;
        if (setOperationOptions) {
            options.InferOutputSchema(std::is_base_of_v<::google::protobuf::Message, TRow>);
        } else {
            TConfig::Get()->InferTableSchema = std::is_base_of_v<::google::protobuf::Message, TRow>;
        }

        {
            auto writer = client->CreateTableWriter<TRow>(workingDir + "/input");
            TRow row;
            row.SetHost("build01-myt.yandex.net");
            row.SetPath("~/.virmc");
            row.SetHttpCode(3213);
            writer->AddRow(row);
            writer->Finish();
        }

        auto checkSchema = [] (TNode schemaNode) {
            TTableSchema schema;
            Deserialize(schema, schemaNode);
            UNIT_ASSERT(AreSchemasEqual(
                schema,
                TTableSchema()
                    .AddColumn(TColumnSchema().Name("Host").Type(EValueType::VT_STRING))
                    .AddColumn(TColumnSchema().Name("Path").Type(EValueType::VT_STRING))
                    .AddColumn(TColumnSchema().Name("HttpCode").Type(EValueType::VT_INT32))));
        };

        client->Map(
            TMapOperationSpec()
                .template AddInput<TRow>(workingDir + "/input")
                .template AddOutput<TRow>(workingDir + "/map_output"),
            new TIdMapper,
            options);

        checkSchema(client->Get(workingDir + "/map_output/@schema"));

        client->MapReduce(
            TMapReduceOperationSpec()
                .template AddInput<TRow>(workingDir + "/input")
                .template AddOutput<TRow>(workingDir + "/mapreduce_output")
                .ReduceBy("Host"),
            new TIdMapper,
            new TIdReducer,
            options);

        checkSchema(client->Get(workingDir + "/mapreduce_output/@schema"));
    }

    Y_UNIT_TEST(ProtobufSchemaInference_Config)
    {
        TestSchemaInference<TUrlRow, TUrlRowIdMapper, TUrlRowIdReducer>(false);
    }

    Y_UNIT_TEST(ProtobufSchemaInference_Options)
    {
        TestSchemaInference<TUrlRow, TUrlRowIdMapper, TUrlRowIdReducer>(true);
    }

    Y_UNIT_TEST(YdlSchemaInference)
    {
        TestSchemaInference<NYdlRows::TUrlRow, TYdlUrlRowIdMapper, TYdlUrlRowIdReducer>(true);
    }

    Y_UNIT_TEST(OutputTableCounter)
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

                auto reader = client->CreateTableReader<TNode>(workingDir + "/output1");
                UNIT_ASSERT(reader->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("result", 1));
                reader->Next();
                UNIT_ASSERT(!reader->IsValid());
        }

        {
            client->Reduce(
                TReduceOperationSpec()
                    .ReduceBy("key")
                    .AddInput<TNode>(workingDir + "/input")
                    .AddOutput<TNode>(workingDir + "/output1")
                    .AddOutput<TNode>(workingDir + "/output2"),
                new TReducerThatCountsOutputTables());

                auto reader = client->CreateTableReader<TNode>(workingDir + "/output1");
                UNIT_ASSERT(reader->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("result", 2));
                reader->Next();
                UNIT_ASSERT(!reader->IsValid());
        }
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
            return std::abs(client->Get(weightPath).AsDouble() - 10.0) < 1e-9;
        });

        op3->AbortOperation();
    }

    Y_UNIT_TEST(AllocatedPorts)
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
            UNIT_ASSERT(stream.ReadLine(line));
            const auto port = FromString<ui16>(line);
            UNIT_ASSERT_GT(port, 1023);
        }
        UNIT_ASSERT(!stream.ReadLine(line));
    }

    Y_UNIT_TEST(UnrecognizedSpecWarnings)
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
        auto prefix = AsStringBuf("WARNING! Unrecognized spec for operation");
        for (TStringBuf line : StringSplitter(stream.Str()).Split('\n')) {
            if (line.StartsWith(prefix)) {
                unrecognizedSpec = NodeFromYsonString(line.After(':'));
                break;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(unrecognizedSpec.GetType(), TNode::Map);
        UNIT_ASSERT_VALUES_EQUAL(
            unrecognizedSpec,
            TNode()
                ("mapper", TNode()("blah1", 1))
                ("blah2", 2));
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

    Y_UNIT_TEST(NewMapReduceOverloads)
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
        const std::vector<TNode> expected = {TNode()("key", "foo")("value", "7")};
        auto readOutputAndRemove = [&] () {
            auto reader = client->CreateTableReader<TNode>(workingDir + "/output");
            std::vector<TNode> result;
            for (; reader->IsValid(); reader->Next()) {
                result.push_back(reader->GetRow());
            }
            client->Remove(workingDir + "/output");
            return result;
        };

        client->Map(
            new TIdMapper,
            workingDir + "/input",
            workingDir + "/output");
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->Reduce(
            new TIdReducer,
            workingDir + "/input",
            workingDir + "/output",
            "key");
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->MapReduce(
            new TIdMapper,
            new TIdReducer,
            workingDir + "/input",
            workingDir + "/output",
            "key");
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);

        client->MapReduce(
            new TIdMapper,
            new TIdReducer,
            new TIdReducer,
            workingDir + "/input",
            workingDir + "/output",
            "key");
        UNIT_ASSERT_VALUES_EQUAL(readOutputAndRemove(), expected);
    }

    Y_UNIT_TEST(NewSortOverload)
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
        const std::vector<TNode> expected = {TNode()("key", "bar"), TNode()("key", "foo")};
        auto readOutput = [&] () {
            auto reader = client->CreateTableReader<TNode>(workingDir + "/output");
            std::vector<TNode> result;
            for (; reader->IsValid(); reader->Next()) {
                result.push_back(reader->GetRow());
            }
            return result;
        };

        client->Sort(
            workingDir + "/input",
            workingDir + "/output",
            "key");
        UNIT_ASSERT_VALUES_EQUAL(readOutput(), expected);
    }

    Y_UNIT_TEST(OperationTimeout)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable = TRichYPath(workingDir + "/input");
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("key", 1));
        writer->Finish();

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(inputTable)
                    .AddOutput<TNode>(workingDir + "/output")
                    .TimeLimit(TDuration::Seconds(2)),
                new TSleepingMapper(TDuration::Seconds(3))),
            TOperationFailedError,
            "Operation is running for too long");
    }

    Y_UNIT_TEST(JobTimeout)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable = TRichYPath(workingDir + "/input");
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("key", 1));
        writer->Finish();

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            client->Map(
                TMapOperationSpec()
                    .AddInput<TNode>(inputTable)
                    .AddOutput<TNode>(workingDir + "/output")
                    .MapperSpec(TUserJobSpec().JobTimeLimit(TDuration::Seconds(2))),
                new TSleepingMapper(TDuration::Seconds(3))),
            TOperationFailedError,
            "Job time limit exceeded");
    }

    Y_UNIT_TEST(QLFilter)
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
                UNIT_ASSERT_VALUES_EQUAL(cursor.GetRow(), TNode()("foo", i));
                ++i;
            }
        }
    }

    Y_UNIT_TEST(QLAndColumnFilter)
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

        UNIT_ASSERT_EXCEPTION_CONTAINS(
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

    Y_UNIT_TEST(CommandRawJob)
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

        auto reader = client->CreateTableReader<TNode>(outputTable);
        TVector<TNode> rows;
        for (const auto& cursor : *reader) {
            rows.push_back(cursor.GetRow());
        }
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0], TNode()("a", "koo")("b", "kindzadza"));
    }

    Y_UNIT_TEST(CommandVanillaJob)
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
        UNIT_ASSERT_VALUES_EQUAL(is.ReadAll(), "Hello world!\n");
    }

} // Y_UNIT_TEST_SUITE(Operations)

////////////////////////////////////////////////////////////////////////////////

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
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompleted();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Completed);
    }

    Y_UNIT_TEST(WaitAllCompleted_ErrorOperations)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
        tracker.AddOperation(op2);

        UNIT_ASSERT_EXCEPTION(tracker.WaitAllCompleted(), TOperationFailedError);
    }

    Y_UNIT_TEST(WaitAllCompletedOrError_OkOperations)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompletedOrError();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Completed);
    }

    Y_UNIT_TEST(WaitAllCompletedOrError_ErrorOperations)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
        tracker.AddOperation(op2);

        tracker.WaitAllCompletedOrError();
        UNIT_ASSERT_VALUES_EQUAL(op1->GetBriefState(), EOperationBriefState::Completed);
        UNIT_ASSERT_VALUES_EQUAL(op2->GetBriefState(), EOperationBriefState::Failed);
    }

    Y_UNIT_TEST(WaitOneCompleted_OkOperation)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output2");
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
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
        tracker.AddOperation(op2);

        auto waitByOne = [&] {
            auto waited1 = tracker.WaitOneCompleted();
            auto waited2 = tracker.WaitOneCompleted();
        };

        UNIT_ASSERT_EXCEPTION(waitByOne(), TOperationFailedError);
    }

    Y_UNIT_TEST(WaitOneCompletedOrError_ErrorOperation)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        CreateTableWithFooColumn(client, workingDir + "/input");

        TOperationTracker tracker;

        auto op1 = AsyncSortByFoo(client, workingDir + "/input", workingDir + "/output1");
        tracker.AddOperation(op1);
        auto op2 = AsyncAlwaysFailingMapper(client, workingDir + "/input", workingDir + "/output2");
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
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->EnableDebugMetrics = true;
        TConfig::Get()->RetryCount = 1;
        TConfig::Get()->ReadRetryCount = 1;
        TConfig::Get()->StartOperationRetryCount = 1;
        TConfig::Get()->WaitLockPollInterval = TDuration::MilliSeconds(0);


        CreateTableWithFooColumn(client, workingDir + "/input");
        auto tx = client->StartTransaction();

        auto op = tx->Map(
            TMapOperationSpec()
            .AddInput<TNode>(workingDir + "/input")
            .AddOutput<TNode>(workingDir + "/output"),
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

} // Y_UNIT_TEST_SUITE(OperationTracker)

////////////////////////////////////////////////////////////////////////////////

// This mapper maps `n` input tables to `2n` output tables.
// First `n` tables are duplicated into outputs `0,2,...,2n-2` and `1,3,...,2n-1`,
// adding "int64" columns "even" and "odd" to schemas correspondingly.
class TInferringNodeMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto even = reader->MoveRow();
            auto odd = even;
            even["even"] = 100;
            odd["odd"] = 101;
            writer->AddRow(even, 2 * reader->GetTableIndex());
            writer->AddRow(odd, 2 * reader->GetTableIndex() + 1);
        }
    }

    void InferSchemas(const ISchemaInferenceContext& context, TSchemaInferenceResultBuilder& builder) const override
    {
        for (int i = 0; i < context.GetInputTableCount(); ++i) {
            auto schema = context.GetInputTableSchema(i);
            schema.AddColumn(TColumnSchema().Name("even").Type(EValueType::VT_INT64));
            builder.OutputSchema(2 * i, schema);
            schema.MutableColumns().back() = TColumnSchema().Name("odd").Type(EValueType::VT_INT64);
            builder.OutputSchema(2 * i + 1, schema);
        }
    }
};
REGISTER_MAPPER(TInferringNodeMapper);

// This mapper sends all the input rows into the 0-th output stream.
// Moreover, a row from i-th table is sent to (i + 1)-th output stream.
// Schema for 0-th table is the result of concatenation of all the input schemas,
// other output schemas are copied as-is.
class TInferringMapperForMapReduce : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow(), 0);
            writer->AddRow(reader->MoveRow(), 1 + reader->GetTableIndex());
        }
    }

    void InferSchemas(const ISchemaInferenceContext& context, TSchemaInferenceResultBuilder& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputTableCount() + 1, context.GetOutputTableCount());

        TTableSchema bigSchema;
        for (int i = 0; i < context.GetInputTableCount(); ++i) {
            auto schema = context.GetInputTableSchema(i);
            UNIT_ASSERT(!schema.Empty());
            builder.OutputSchema(i + 1, schema);
            for (const auto& column : schema.Columns()) {
                bigSchema.AddColumn(column);
            }
        }
        builder.OutputSchema(0, bigSchema);
    }
};
REGISTER_MAPPER(TInferringMapperForMapReduce);

// This reduce combiner retains only the columns from `ColumnsToRetain_`.
class TInferringReduceCombiner : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TInferringReduceCombiner() = default;

    TInferringReduceCombiner(THashSet<TString> columnsToRetain)
        : ColumnsToRetain_(std::move(columnsToRetain))
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->MoveRow();
            TNode out = TNode::CreateMap();
            for (const auto& toRetain : ColumnsToRetain_) {
                if (row.HasKey(toRetain)) {
                    out[toRetain] = std::move(row[toRetain]);
                }
            }
            writer->AddRow(out);
        }
    }

    void InferSchemas(const ISchemaInferenceContext& context, TSchemaInferenceResultBuilder& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputTableCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.GetOutputTableCount(), 1);
        UNIT_ASSERT(!context.GetInputTableSchema(0).Empty());

        TTableSchema result;
        for (const auto& column : context.GetInputTableSchema(0).Columns()) {
            if (ColumnsToRetain_.contains(column.Name())) {
                result.AddColumn(column);
            }
        }
        builder.OutputSchema(0, result);
    }

    Y_SAVELOAD_JOB(ColumnsToRetain_);

private:
    THashSet<TString> ColumnsToRetain_;
};
REGISTER_REDUCER(TInferringReduceCombiner);

// The reducer just outputs the passed rows. Schema is copied as-is.
class TInferringIdReducer : public TIdReducer
{
public:
    void InferSchemas(const ISchemaInferenceContext& context, TSchemaInferenceResultBuilder& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputTableCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.GetOutputTableCount(), 1);
        UNIT_ASSERT(!context.GetInputTableSchema(0).Empty());
        builder.OutputSchema(0, context.GetInputTableSchema(0));
    }
};
REGISTER_REDUCER(TInferringIdReducer);

// This mapper infers one additional column.
template<class TBase>
class TInferringMapper : public TBase
{
public:
    void InferSchemas(const ISchemaInferenceContext& context, TSchemaInferenceResultBuilder& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputTableCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.GetOutputTableCount(), 1);

        auto schema = context.GetInputTableSchema(0);
        UNIT_ASSERT(!schema.Empty());

        schema.AddColumn("extra", EValueType::VT_DOUBLE);
        builder.OutputSchema(0, schema);
    }
};
REGISTER_MAPPER(TInferringMapper<TUrlRowIdMapper>);
REGISTER_MAPPER(TInferringMapper<TYdlUrlRowIdMapper>);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(JobSchemaInference)
{
    Y_UNIT_TEST(Map)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto someSchema = TTableSchema().AddColumn("some_column", EValueType::VT_STRING);
        auto otherSchema = TTableSchema().AddColumn("other_column", EValueType::VT_INT64);

        TYPath someTable = workingDir + "/some_table";
        TYPath otherTable = workingDir + "/other_table";
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(someTable).Schema(someSchema));
            writer->AddRow(TNode()("some_column", "abc"));
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(otherTable).Schema(otherSchema));
            writer->AddRow(TNode()("other_column", 12));
            writer->Finish();
        }

        TMapOperationSpec spec;
        spec.AddInput<TNode>(someTable);
        spec.AddInput<TNode>(otherTable);

        TVector<TYPath> outTables;
        for (int i = 0; i < 4; ++i) {
            outTables.push_back(workingDir + "/out_table_" + ToString(i));
            spec.AddOutput<TNode>(outTables.back());
        }

        client->Map(spec, new TInferringNodeMapper());

        TVector<TTableSchema> outSchemas;
        for (const auto& path : outTables) {
            outSchemas.emplace_back();
            Deserialize(outSchemas.back(), client->Get(path + "/@schema"));
        }

        for (int i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns().size(), 2);

            if (i < 2) {
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Name(), someSchema.Columns()[0].Name());
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Type(), someSchema.Columns()[0].Type());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Name(), otherSchema.Columns()[0].Name());
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Type(), otherSchema.Columns()[0].Type());
            }

            UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[1].Name(), (i % 2 == 0) ? "even" : "odd");
            UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[1].Type(), EValueType::VT_INT64);
        }
    }

    Y_UNIT_TEST(MapReduce)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto someSchema = TTableSchema()
            .AddColumn("some_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING)
            .AddColumn("some_column", EValueType::VT_STRING);
        auto otherSchema = TTableSchema()
            .AddColumn("other_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING)
            .AddColumn(TColumnSchema().Name("other_column").Type(EValueType::VT_INT64));

        TYPath someTable = workingDir + "/some_table";
        TYPath otherTable = workingDir + "/other_table";
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(someTable).Schema(someSchema));
            writer->AddRow(TNode()("some_column", "abc"));
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(otherTable).Schema(otherSchema));
            writer->AddRow(TNode()("other_column", 12));
            writer->Finish();
        }

        auto spec = TMapReduceOperationSpec()
            .AddInput<TNode>(someTable)
            .AddInput<TNode>(otherTable)
            .SortBy({"some_key", "other_key"})
            .MaxFailedJobCount(1)
            .ForceReduceCombiners(true);

        TVector<TYPath> mapperOutTables;
        for (int i = 0; i < 2; ++i) {
            mapperOutTables.push_back(workingDir + "/mapper_out_table_" + ToString(i));
            spec.AddMapOutput<TNode>(mapperOutTables.back());
        }

        TYPath outTable = workingDir + "/out_table";
        spec.AddOutput<TNode>(outTable);

        THashSet<TString> toRetain = {"some_key", "other_key", "other_column"};
        client->MapReduce(
            spec,
            new TInferringMapperForMapReduce(),
            new TInferringReduceCombiner(toRetain),
            new TInferringIdReducer());

        TVector<TTableSchema> mapperOutSchemas;
        for (const auto& path : mapperOutTables) {
            mapperOutSchemas.emplace_back();
            Deserialize(mapperOutSchemas.back(), client->Get(path + "/@schema"));
        }
        TTableSchema outSchema;
        Deserialize(outSchema, client->Get(outTable + "/@schema"));

        for (const auto& [index, expectedSchema] : TVector<std::pair<int, TTableSchema>>{{0, someSchema}, {1, otherSchema}}) {
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns().size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[0].Name(), expectedSchema.Columns()[0].Name());
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[0].Type(), expectedSchema.Columns()[0].Type());
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[0].SortOrder(), expectedSchema.Columns()[0].SortOrder());

            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[1].Name(), expectedSchema.Columns()[1].Name());
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[1].Type(), expectedSchema.Columns()[1].Type());
        }

        UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns().size(), 3);

        for (const auto& [index, expectedName, expectedType, expectedSortOrder] :
            TVector<std::tuple<int, TString, EValueType, TMaybe<ESortOrder>>>{
                {0, "some_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING},
                {1, "other_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING},
                {2, "other_column", EValueType::VT_INT64, Nothing()}})
        {
            UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns()[index].Name(), expectedName);
            UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns()[index].Type(), expectedType);
            UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns()[index].SortOrder(), expectedSortOrder);
        }
    }

    template<typename TRow, class TIdMapper, class TInferringMapper>
    void TestPrecedenceOverInference()
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TYPath input = workingDir + "/input";
        {
            auto path = TRichYPath(input);
            if constexpr (std::is_base_of_v<::google::protobuf::Message, TRow>) {
                path = WithSchema<TRow>(TRichYPath(input));
            }
            auto writer = client->CreateTableWriter<TRow>(path);
            TRow row;
            row.SetHost("ya.ru");
            row.SetPath("search");
            row.SetHttpCode(404);
            writer->AddRow(row);
            writer->Finish();
        }

        TYPath outputForInference = workingDir + "/output_for_inference";
        client->Map(
            TMapOperationSpec()
                .template AddInput<TRow>(input)
                .template AddOutput<TRow>(outputForInference),
            new TIdMapper(),
            TOperationOptions()
                .InferOutputSchema(std::is_base_of_v<::google::protobuf::Message, TRow>));

        {
            TTableSchema schema;
            Deserialize(schema, client->Get(outputForInference + "/@schema"));

            UNIT_ASSERT_VALUES_EQUAL(schema.Columns().size(), 3);
            for (const auto& [index, expectedName, expectedType] : TVector<std::tuple<int, TString, EValueType>>{
                {0, "Host", EValueType::VT_STRING},
                {1, "Path", EValueType::VT_STRING},
                {2, "HttpCode", EValueType::VT_INT32}})
            {
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Name(), expectedName);
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Type(), expectedType);
            }
        }

        TYPath outputForBothInferences = workingDir + "/output_for_both_inferences";
        client->Map(
            TMapOperationSpec()
                .template AddInput<TRow>(input)
                .template AddOutput<TRow>(outputForBothInferences),
            new TInferringMapper(),
            TOperationOptions()
                .InferOutputSchema(std::is_base_of_v<::google::protobuf::Message, TRow>));

        TTableSchema schema;
        Deserialize(schema, client->Get(outputForBothInferences + "/@schema"));

        UNIT_ASSERT_VALUES_EQUAL(schema.Columns().size(), 4);
        for (const auto& [index, expectedName, expectedType] : TVector<std::tuple<int, TString, EValueType>>{
            {0, "Host", EValueType::VT_STRING},
            {1, "Path", EValueType::VT_STRING},
            {2, "HttpCode", EValueType::VT_INT32},
            {3, "extra", EValueType::VT_DOUBLE}})
        {
            UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Name(), expectedName);
            UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Type(), expectedType);
        }
    }

    Y_UNIT_TEST(PrecedenceOverProtobufInference)
    {
        TestPrecedenceOverInference<TUrlRow, TUrlRowIdMapper, TInferringMapper<TUrlRowIdMapper>>();
    }

    Y_UNIT_TEST(PrecedenceOverYdlInference)
    {
        TestPrecedenceOverInference<NYdlRows::TUrlRow, TYdlUrlRowIdMapper, TInferringMapper<TYdlUrlRowIdMapper>>();
    }

} // Y_UNIT_TEST_SUITE(JobSchemaInference)
