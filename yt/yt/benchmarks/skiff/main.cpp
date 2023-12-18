#include "experimental_yson_pull_format.h"

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/library/formats/format.h>
#include <yt/yt/library/formats/skiff_parser.h>
#include <yt/yt/library/formats/skiff_writer.h>
#include <yt/yt/library/formats/protobuf.h>
#include <yt/yt/library/formats/protobuf_writer.h>
#include <yt/yt/library/formats/protobuf_parser.h>
#include <yt/yt/library/formats/yson_parser.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/process/io_dispatcher.h>
#include <yt/yt/library/process/pipe.h>

#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/skiff/skiff_schema.h>

#include <util/random/fast.h>
#include <util/stream/buffered.h>
#include <util/stream/output.h>
#include <util/system/file.h>
#include <util/system/thread.h>

constexpr size_t ROW_COUNT = 10000000;

using namespace NYT;
using namespace NYT::NYTree;
using namespace NYT::NFormats;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBenchmarkedFormat,
    (Skiff)
    (Protobuf)
    (Yson)
    (YsonPull)
);

////////////////////////////////////////////////////////////////////////////////

static TString GenerateRandomString(TFastRng64* rng, size_t size)
{
    TString result;
    result.ReserveAndResize(size + sizeof(ui64));
    char* data = result.Detach();
    for (size_t pos = 0; pos < size; pos += sizeof(ui64)) {
        *(data + pos) = rng->GenRand64();
    }
    result.resize(size);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TFuncThread
    : public ISimpleThread
{
public:
    using TFunc = std::function<void()>;

private:
    TFunc Func;

public:
    TFuncThread(const TFunc& func)
        : Func(func)
    { }

    void* ThreadProc() override
    {
        Func();
        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow GenerateUrldat(TFastRng64* rng, const TNameTablePtr& nameTable)
{
    auto generateTimestamp = [&] {
        // [01.01.2010,  01.01.2018)
        return rng->Uniform(1262304000, 1514764800);
    };
    TUnversionedOwningRowBuilder builder;

    builder.AddValue(MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)));

    { // Host
        const ui32 hostLen = rng->Uniform(5, 20);
        const TString host = GenerateRandomString(rng, hostLen);
        builder.AddValue(
            MakeUnversionedStringValue(
                host,
                nameTable->GetIdOrRegisterName("Host")));
    }

    { // Path
        const ui32 pathLen = rng->Uniform(1, 256);
        const TString path = GenerateRandomString(rng, pathLen);
        builder.AddValue(
            MakeUnversionedStringValue(
                path,
                nameTable->GetIdOrRegisterName("Path")));
    }

    { // Region
        const TString region = GenerateRandomString(rng, 3);
        builder.AddValue(
            MakeUnversionedStringValue(
                region,
                nameTable->GetIdOrRegisterName("Region")));
    }

    { // LastAccess
        const i64 lastAccess = generateTimestamp();
        builder.AddValue(
            MakeUnversionedUint64Value(
                lastAccess,
                nameTable->GetIdOrRegisterName("LastAccess")));
    }

    { // ExportTime
        const i64 exportTime = generateTimestamp();
        builder.AddValue(
            MakeUnversionedUint64Value(
                exportTime,
                nameTable->GetIdOrRegisterName("ExportTime")));
    }

    { // TextCRC
        if (rng->Uniform(100) < 10) {
            builder.AddValue(
                MakeUnversionedSentinelValue(
                    EValueType::Null,
                    nameTable->GetIdOrRegisterName("TextCRC")));
        } else {
            auto textCrc = rng->GenRand64();
            builder.AddValue(
                MakeUnversionedUint64Value(
                    textCrc,
                    nameTable->GetIdOrRegisterName("TextCRC")));
        }
    }

    { // HttpModTime
        const i64 httpModTime = generateTimestamp();
        builder.AddValue(
            MakeUnversionedUint64Value(
                httpModTime,
                nameTable->GetIdOrRegisterName("HttpModTime")));
    }

    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

struct TParserWriterPair
{
    std::unique_ptr<IParser> Parser;
    ISchemalessFormatWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

struct IDataset
{
    virtual ~IDataset() = default;

    virtual const std::vector<TUnversionedOwningRow>& GetData() const = 0;
    virtual const TNameTablePtr& GetNameTable() const = 0;

    virtual std::unique_ptr<IParser> CreateYsonParser(IValueConsumer* valueConsumer) const = 0;
    virtual std::unique_ptr<IParser> CreateYsonPullParser(IValueConsumer* valueConsumer) const = 0;
    virtual ISchemalessFormatWriterPtr CreateYsonWriter(
        const TNameTablePtr& datasetNameTable,
        NConcurrency::IAsyncOutputStreamPtr writerStream) const = 0;

    virtual std::unique_ptr<IParser> CreateSkiffParser(IValueConsumer* valueConsumer) const = 0;
    virtual ISchemalessFormatWriterPtr CreateSkiffWriter(
        const TNameTablePtr& datasetNameTable,
        NConcurrency::IAsyncOutputStreamPtr writerStream) const = 0;

    virtual std::unique_ptr<IParser> CreateProtobufParser(IValueConsumer* valueConsumer) const = 0;
    virtual ISchemalessFormatWriterPtr CreateProtobufWriter(
        const TNameTablePtr& datasetNameTable,
        NConcurrency::IAsyncOutputStreamPtr writerStream) const = 0;

    std::unique_ptr<IParser> CreateParser(EBenchmarkedFormat format, IValueConsumer* valueConsumer) const
    {
        switch (format) {
            case EBenchmarkedFormat::Protobuf:
                return CreateProtobufParser(valueConsumer);
            case EBenchmarkedFormat::Skiff:
                return CreateSkiffParser(valueConsumer);
            case EBenchmarkedFormat::Yson:
                return CreateYsonParser(valueConsumer);
            case EBenchmarkedFormat::YsonPull:
                return CreateYsonPullParser(valueConsumer);
            default:
                YT_ABORT();
        }
    }

    ISchemalessFormatWriterPtr CreateWriter(
        EBenchmarkedFormat format,
        const TNameTablePtr& datasetNameTable,
        NConcurrency::IAsyncOutputStreamPtr writerStream) const
    {
        switch (format) {
            case EBenchmarkedFormat::Protobuf:
                return CreateProtobufWriter(
                    datasetNameTable,
                    writerStream);
            case EBenchmarkedFormat::Skiff:
                return CreateSkiffWriter(
                    datasetNameTable,
                    writerStream);
            case EBenchmarkedFormat::Yson:
            case EBenchmarkedFormat::YsonPull:
                return CreateYsonWriter(
                    datasetNameTable,
                    writerStream);
            default:
                YT_ABORT();
        }
    }
};

class TUrldatDataset
    : public IDataset
{
public:
    TUrldatDataset(std::unique_ptr<TFastRng64> rng, size_t size)
        : Rng_(std::move(rng))
        , Size_(size)
    { }

    const std::vector<TUnversionedOwningRow>& GetData() const override
    {
        if (Data_.empty()) {
            GenerateData();
        }
        return Data_;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const INodePtr YsonConfigNode = BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("format")
            .Value("binary")
        .EndAttributes()
        .Value("yson");

    const INodePtr YsonPullConfigNode = BuildYsonNodeFluently().Value("yson_pull");

    std::unique_ptr<IParser> CreateYsonParser(IValueConsumer* valueConsumer) const override
    {
        TFormat format;
        Deserialize(format, YsonConfigNode);
        return CreateParserForFormat(
            format,
            valueConsumer);
    }

    std::unique_ptr<IParser> CreateYsonPullParser(IValueConsumer* valueConsumer) const override
    {
        return CreateParserForYsonPull(
            valueConsumer,
            ConvertTo<TYsonFormatConfigPtr>(YsonPullConfigNode->Attributes()),
            0);
    }

    ISchemalessFormatWriterPtr CreateYsonWriter(
        const TNameTablePtr& datasetNameTable,
        NConcurrency::IAsyncOutputStreamPtr writerStream) const override
    {
        TFormat format;
        Deserialize(format, YsonConfigNode);
        return CreateStaticTableWriterForFormat(
            format,
            datasetNameTable,
            {},
            writerStream,
            true,
            New<TControlAttributesConfig>(),
            0);
    }

    const INodePtr ProtobufFormatConfig = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("Host")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("string")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Path")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("string")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Region")
                            .Item("field_number").Value(3)
                            .Item("proto_type").Value("string")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("LastAccess")
                            .Item("field_number").Value(4)
                            .Item("proto_type").Value("uint64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("ExportTime")
                            .Item("field_number").Value(5)
                            .Item("proto_type").Value("uint64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("TextCRC")
                            .Item("field_number").Value(6)
                            .Item("proto_type").Value("fixed64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("HttpModTime")
                            .Item("field_number").Value(7)
                            .Item("proto_type").Value("uint64")
                        .EndMap()

                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    std::unique_ptr<IParser> CreateProtobufParser(IValueConsumer* valueConsumer) const override
    {
        auto config = New<NFormats::TProtobufFormatConfig>();
        config->Load(ProtobufFormatConfig);
        return CreateParserForProtobuf(
            valueConsumer,
            config,
            0);
    }

    ISchemalessFormatWriterPtr CreateProtobufWriter(
        const TNameTablePtr& datasetNameTable,
        NConcurrency::IAsyncOutputStreamPtr writerStream) const override
    {
        auto config = New<NFormats::TProtobufFormatConfig>();
        config->Load(ProtobufFormatConfig);
        return CreateWriterForProtobuf(
            config,
            {New<TTableSchema>()},
            datasetNameTable,
            writerStream,
            true,
            New<TControlAttributesConfig>(),
            0);
    }

    std::shared_ptr<NSkiff::TSkiffSchema> SkiffSchema = NSkiff::CreateTupleSchema({
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::String32)->SetName("Host"),
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::String32)->SetName("Path"),
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::String32)->SetName("Region"),
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint64)->SetName("LastAccess"),
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint64)->SetName("ExportTime"),
        NSkiff::CreateVariant8Schema({
            NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Nothing),
            NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint64),
            })->SetName("TextCRC"),
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint64)->SetName("HttpModTime"),
    });

    std::unique_ptr<IParser> CreateSkiffParser(IValueConsumer* valueConsumer) const override
    {
        return CreateParserForSkiff(SkiffSchema, valueConsumer);
    }

    ISchemalessFormatWriterPtr CreateSkiffWriter(
        const TNameTablePtr& datasetNameTable,
        NConcurrency::IAsyncOutputStreamPtr writerStream) const override
    {
        return CreateWriterForSkiff(
            {SkiffSchema},
            datasetNameTable,
            {},
            writerStream,
            true,
            New<TControlAttributesConfig>(),
            0);
    }

private:
    void GenerateData() const
    {
        Data_.reserve(Size_);

        for (size_t i = 0; i != Size_; ++i) {
            Data_.push_back(GenerateUrldat(Rng_.get(), NameTable_));
        }
    }

private:
    mutable std::vector<TUnversionedOwningRow> Data_;
    TNameTablePtr NameTable_ = New<TNameTable>();
    std::unique_ptr<TFastRng64> Rng_;
    const size_t Size_ = 0;
};

class TDevnullValueConsumer
    : public IValueConsumer
{
public:
    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        static const auto schema = New<TTableSchema>();
        return schema;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    { }

    void OnValue(const TUnversionedValue& /*value*/) override
    { }

    void OnEndRow() override
    { }

private:
    TNameTablePtr NameTable_ = New<TNameTable>();
};

class TMyWritingValueConsumer
    : public IValueConsumer
{
public:
    TMyWritingValueConsumer(TNameTablePtr nameTable, ISchemalessFormatWriterPtr writer)
        : NameTable_(nameTable)
        , Writer_(writer)
    { }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        static const auto schema = New<TTableSchema>();
        return schema;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    {
        Builder_.AddValue(MakeUnversionedUint64Value(0, TableIndexId_));
    }

    void OnValue(const TUnversionedValue& value) override
    {
        Builder_.AddValue(value);
    }

    void OnEndRow() override
    {
        auto owningRow = Builder_.FinishRow();
        TUnversionedRow row[] = {owningRow};
        Writer_->Write(row);
    }

private:
    TNameTablePtr NameTable_ = New<TNameTable>();
    TUnversionedOwningRowBuilder Builder_;
    ISchemalessFormatWriterPtr Writer_ = nullptr;
    int TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
};

class TCountingValueConsumer
    : public IValueConsumer
{
public:
    explicit TCountingValueConsumer(TNameTablePtr nameTable)
        : NameTable_(std::move(nameTable))
    { }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        static const auto schema = New<TTableSchema>();
        return schema;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    { }

    void OnValue(const TUnversionedValue& /*value*/) override
    {
        ++Count_;
    }

    void OnEndRow() override
    { }

    i64 GetCount() const
    {
        return Count_;
    }

private:
    const TNameTablePtr NameTable_;
    i64 Count_ = 0;
};

void GenerateCodeDecode(EBenchmarkedFormat format, const IDataset& dataset)
{
    NPipes::TPipeFactory factory;
    NPipes::TPipe pipe = factory.Create();

    auto writerStream = pipe.CreateAsyncWriter();
    auto readerStream = pipe.CreateAsyncReader();

    TFastRng64 rng(42);

    const auto& data = dataset.GetData();

    TDevnullValueConsumer devnullConsumer;
    auto parser = dataset.CreateParser(format, &devnullConsumer);
    auto writer = dataset.CreateWriter(format, dataset.GetNameTable(), writerStream);

    auto writerProc = [&] {
        for (const auto& value : data) {
            TUnversionedRow row = value;
            writer->Write({row});
        }
        writer->Close()
            .Get()
            .ThrowOnError();
        writerStream->Close().Get().ThrowOnError();
        pipe.CloseWriteFD();
    };

    auto readerProc = [&] {
        auto buf = TSharedMutableRef::Allocate(1024 * 1024, {.InitializeStorage = false});

        while (size_t read = readerStream->Read(buf).Get().ValueOrThrow()) {
            parser->Read(TStringBuf(&buf.Front(), read));
        }
        parser->Finish();
    };

    NProfiling::TWallTimer timer;

    TFuncThread writerThread(writerProc);
    TFuncThread readerThread(readerProc);
    writerThread.Start();
    readerThread.Start();
    writerThread.Join();
    readerThread.Join();

    Cerr << ToString(format) << " took: " << timer.GetElapsedTime() << Endl;
}

void GenerateCode(EBenchmarkedFormat format, const IDataset& dataset, int writerRunCount)
{
    auto writerStream = NNet::CreateConnectionFromFD(1, {}, {}, NPipes::TIODispatcher::Get()->GetPoller());

    const auto& owningData = dataset.GetData();
    std::vector<TUnversionedRow> data;
    for (const auto& row : owningData) {
        data.emplace_back(row);
    }

    auto writer = dataset.CreateWriter(format, dataset.GetNameTable(), writerStream);

    NProfiling::TWallTimer timer;

    const int rangeSize = 1000;
    for (int i = 0; i < writerRunCount; ++i) {
        auto it = data.data();
        while (it != data.data() + data.size()) {
            auto next = std::min(it + rangeSize, data.data() + data.size());
            TRange<TUnversionedRow> range(it, next);
            writer->Write(range);
            it = next;
        }
    }
    writer->Close()
        .Get()
        .ThrowOnError();
    writerStream->Close().Get().ThrowOnError();

    Cerr << ToString(format) << " took: " << timer.GetElapsedTime() << Endl;
}

void Parse(EBenchmarkedFormat format, const IDataset& dataset)
{
    NProfiling::TWallTimer timer;

    auto readerStream = NNet::CreateConnectionFromFD(0, {}, {}, NPipes::TIODispatcher::Get()->GetPoller());

    TCountingValueConsumer consumer(dataset.GetNameTable());
    auto parser = dataset.CreateParser(format, &consumer);

    auto buf = TSharedMutableRef::Allocate(1024 * 1024, {.InitializeStorage = false});
    while (size_t read = readerStream->Read(buf).Get().ValueOrThrow()) {
        parser->Read(TStringBuf(&buf.Front(), read));
    }
    parser->Finish();

    Cout << "Parsed " << consumer.GetCount() << " values" << Endl;
    Cerr << ToString(format) << " took: " << timer.GetElapsedTime() << Endl;
}

void ParseWrite(EBenchmarkedFormat format, const IDataset& dataset)
{
    NProfiling::TWallTimer timer;

    auto readerStream = NNet::CreateConnectionFromFD(0, {}, {}, NPipes::TIODispatcher::Get()->GetPoller());
    auto writerStream = NNet::CreateConnectionFromFD(1, {}, {}, NPipes::TIODispatcher::Get()->GetPoller());

    auto writer = dataset.CreateWriter(format, dataset.GetNameTable(), writerStream);
    TMyWritingValueConsumer consumer(dataset.GetNameTable(), writer);
    auto parser = dataset.CreateParser(format, &consumer);

    auto buf = TSharedMutableRef::Allocate(1024 * 1024, {.InitializeStorage = false});
    while (size_t read = readerStream->Read(buf).Get().ValueOrThrow()) {
        parser->Read(TStringBuf(&buf.Front(), read));
    }
    parser->Finish();
    writer->Close().Get().ThrowOnError();
    writerStream->Close().Get().ThrowOnError();

    Cerr << ToString(format) << " took: " << timer.GetElapsedTime() << Endl;
}


int main(int argc, const char** argv)
{
    auto opts = NLastGetopt::TOpts::Default();
    opts.AddHelpOption();
    opts.AddLongOption("generate-write", "generate dataset and serialize it to stdout")
        .RequiredArgument("FORMAT");

    opts.AddLongOption("parse", "parse values from stdin and print some statistics on them")
        .RequiredArgument("FORMAT");

    opts.AddLongOption("parse-write", "parse values from stdin and code them back to stdout")
        .RequiredArgument("FORMAT");

    opts.AddLongOption("generate-write-parse", "generate dataset serialize it to pipe and parse it, do it for all formats")
        .NoArgument();

    opts.AddLongOption("writer-runs", "how many times to repeat writing of generated data")
        .RequiredArgument("NUMBER");

    NLastGetopt::TOptsParseResult args(&opts, argc, argv);

    TUrldatDataset dataset(std::make_unique<TFastRng64>(42), ROW_COUNT);
    if (args.Has("generate-write")) {
        auto format = ConvertTo<EBenchmarkedFormat>(args.Get<TString>("generate-write"));
        auto writerRunCount = args.Get<int>("writer-runs");
        GenerateCode(format, dataset, writerRunCount);
    } else if (args.Has("parse-write")) {
        auto format = ConvertTo<EBenchmarkedFormat>(args.Get<TString>("parse-write"));
        ParseWrite(format, dataset);
    } else if (args.Has("parse")) {
        auto format = ConvertTo<EBenchmarkedFormat>(args.Get<TString>("parse"));
        Parse(format, dataset);
    } else if (args.Has("generate-write-parse")) {
        GenerateCodeDecode(EBenchmarkedFormat::Skiff, dataset);
        GenerateCodeDecode(EBenchmarkedFormat::YsonPull, dataset);
        GenerateCodeDecode(EBenchmarkedFormat::Yson, dataset);
        GenerateCodeDecode(EBenchmarkedFormat::Protobuf, dataset);
    }

    return 0;
}
