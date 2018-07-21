#include "format.h"
#include "parser.h"
#include "dsv_parser.h"
#include "dsv_writer.h"
#include "protobuf_parser.h"
#include "protobuf_writer.h"
#include "schemaful_dsv_parser.h"
#include "schemaful_dsv_writer.h"
#include "schemaful_writer.h"
#include "schemaless_web_json_writer.h"
#include "schemaless_writer_adapter.h"
#include "skiff_parser.h"
#include "skiff_writer.h"
#include "versioned_writer.h"
#include "yamred_dsv_parser.h"
#include "yamred_dsv_writer.h"
#include "yamr_parser.h"
#include "yamr_writer.h"
#include "yson_parser.h"

#include <yt/core/misc/error.h>

#include <yt/core/yson/writer.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/forwarding_consumer.h>

#include <yt/core/json/json_parser.h>
#include <yt/core/json/json_writer.h>

#include <yt/client/table_client/name_table.h>

#include <yt/ytlib/table_client/schemaful_writer_adapter.h>
#include <yt/ytlib/table_client/table_consumer.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NJson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat()
    : Type_(EFormatType::Null)
    , Attributes_(CreateEphemeralAttributes())
{ }

TFormat::TFormat(EFormatType type, const IAttributeDictionary* attributes)
    : Type_(type)
    , Attributes_(attributes ? attributes->Clone() : CreateEphemeralAttributes())
{ }

TFormat::TFormat(const TFormat& other)
    : Type_(other.Type_)
    , Attributes_(other.Attributes_->Clone())
{ }

TFormat& TFormat::operator=(const TFormat& other)
{
    if (this != &other) {
        Type_ = other.Type_;
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
    }
    return *this;
}

const IAttributeDictionary& TFormat::Attributes() const
{
    return *Attributes_;
}

void Serialize(const TFormat& value, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Items(value.Attributes())
        .EndAttributes()
        .Value(value.GetType());
}

void Deserialize(TFormat& value, INodePtr node)
{
    if (node->GetType() != ENodeType::String) {
        THROW_ERROR_EXCEPTION("Format name must be a string");
    }

    auto typeStr = node->GetValue<TString>();
    EFormatType type;
    try {
        type = ParseEnum<EFormatType>(typeStr);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Invalid format name %Qv",
            typeStr);
    }

    value = TFormat(type, &node->Attributes());
}

////////////////////////////////////////////////////////////////////////////////

namespace {

EYsonType DataTypeToYsonType(EDataType dataType)
{
    switch (dataType) {
        case EDataType::Structured:
            return EYsonType::Node;
        case EDataType::Tabular:
            return EYsonType::ListFragment;
        default:
            THROW_ERROR_EXCEPTION("Data type %Qlv is not supported by YSON",
                dataType);
    }
}

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForYson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IOutputStream* output)
{
    auto config = ConvertTo<TYsonFormatConfigPtr>(&attributes);
    return CreateYsonWriter(
        output,
        config->Format,
        DataTypeToYsonType(dataType),
        config->Format == EYsonFormat::Binary,
        config->BooleanAsString);
}

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForJson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IOutputStream* output)
{
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return CreateJsonConsumer(output, DataTypeToYsonType(dataType), config);
}

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IOutputStream* output)
{
    auto config = ConvertTo<TDsvFormatConfigPtr>(&attributes);
    switch (dataType) {
        case EDataType::Structured:
            return std::unique_ptr<IFlushableYsonConsumer>(new TDsvNodeConsumer(output, config));

        case EDataType::Tabular:
        case EDataType::Binary:
        case EDataType::Null:
            THROW_ERROR_EXCEPTION("Data type %Qlv is not supported by DSV",
                dataType);

        default:
            Y_UNREACHABLE();
    };
}

class TTableParserAdapter
    : public IParser
{
public:
    TTableParserAdapter(
        const TFormat& format,
        std::vector<IValueConsumer*> valueConsumers,
        int tableIndex)
        : TableConsumer_(new TTableConsumer(
            valueConsumers,
            tableIndex))
        , Parser_(CreateParserForFormat(
            format,
            EDataType::Tabular,
            TableConsumer_.get()))
    { }

    virtual void Read(TStringBuf data) override
    {
        Parser_->Read(data);
    }

    virtual void Finish() override
    {
        Parser_->Finish();
    }

private:
    const std::unique_ptr<IYsonConsumer> TableConsumer_;
    const std::unique_ptr<IParser> Parser_;
};

} // namespace

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    IOutputStream* output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateConsumerForYson(dataType, format.Attributes(), output);
        case EFormatType::Json:
            return CreateConsumerForJson(dataType, format.Attributes(), output);
        case EFormatType::Dsv:
            return CreateConsumerForDsv(dataType, format.Attributes(), output);
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %Qlv",
                format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TWriter, class TConsumerAdapter>
TIntrusivePtr<TWriter> CreateAdaptedWriterForYson(
    const IAttributeDictionary& attributes,
    const TTableSchema& schema,
    IAsyncOutputStreamPtr output)
{
    auto config = ConvertTo<TYsonFormatConfigPtr>(&attributes);
    return New<TConsumerAdapter>(std::move(output), schema, [=] (IOutputStream* buffer) {
        if (config->Format == EYsonFormat::Binary) {
            return std::unique_ptr<IFlushableYsonConsumer>(new TBufferedBinaryYsonWriter(
                buffer,
                EYsonType::ListFragment,
                true,
                config->BooleanAsString));
        } else {
            return std::unique_ptr<IFlushableYsonConsumer>(new TYsonWriter(
                buffer,
                config->Format,
                EYsonType::ListFragment));
        }
    });
}

template <class TWriter, class TConsumerAdapter>
TIntrusivePtr<TWriter> CreateAdaptedWriterForJson(
    const IAttributeDictionary& attributes,
    const TTableSchema& schema,
    IAsyncOutputStreamPtr output)
{
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return New<TConsumerAdapter>(std::move(output), schema, [&] (IOutputStream* buffer) {
        return CreateJsonConsumer(buffer, EYsonType::ListFragment, config);
    });
}

ISchemafulWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& format,
    const TTableSchema& schema,
    IAsyncOutputStreamPtr output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateAdaptedWriterForYson<ISchemafulWriter, TSchemafulWriter>(format.Attributes(), schema, std::move(output));
        case EFormatType::Json:
            return CreateAdaptedWriterForJson<ISchemafulWriter, TSchemafulWriter>(format.Attributes(), schema, std::move(output));
        case EFormatType::SchemafulDsv:
            return CreateSchemafulWriterForSchemafulDsv(format.Attributes(), schema, std::move(output));
        case EFormatType::WebJson: {
            auto webJsonFormatConfig = ConvertTo<TSchemalessWebJsonFormatConfigPtr>(&format.Attributes());
            webJsonFormatConfig->SkipSystemColumns = false;

            return CreateSchemafulWriterAdapter(
                CreateSchemalessWriterForWebJson(
                    std::move(webJsonFormatConfig),
                    std::move(output),
                    TNameTable::FromSchema(schema)));
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %Qlv",
                format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

IVersionedWriterPtr CreateVersionedWriterForFormat(
    const TFormat& format,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateAdaptedWriterForYson<IVersionedWriter, TVersionedWriter>(format.Attributes(), schema, output);
        case EFormatType::Json:
            return CreateAdaptedWriterForJson<IVersionedWriter, TVersionedWriter>(format.Attributes(), schema, output);
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %Qlv", format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForFormat(
    const TFormat& format,
    TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    switch (format.GetType()) {
        case EFormatType::Dsv:
            return CreateSchemalessWriterForDsv(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::Yamr:
            return CreateSchemalessWriterForYamr(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::YamredDsv:
            return CreateSchemalessWriterForYamredDsv(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::SchemafulDsv:
            return CreateSchemalessWriterForSchemafulDsv(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::Protobuf:
            return CreateSchemalessWriterForProtobuf(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::WebJson:
            return CreateSchemalessWriterForWebJson(
                format.Attributes(),
                std::move(output),
                nameTable);
        case EFormatType::Skiff:
            return CreateSchemalessWriterForSkiff(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        default:
            auto adapter = New<TSchemalessWriterAdapter>(
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
            adapter->Init(format);
            return adapter;
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateProducerForDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("DSV is supported only for tabular data");
    }
    auto config = ConvertTo<TDsvFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseDsv(input, consumer, config);
    });
}

TYsonProducer CreateProducerForYamr(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("YAMR is supported only for tabular data");
    }
    auto config = ConvertTo<TYamrFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseYamr(input, consumer, config);
    });
}

TYsonProducer CreateProducerForYamredDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("Yamred DSV is supported only for tabular data");
    }
    auto config = ConvertTo<TYamredDsvFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseYamredDsv(input, consumer, config);
    });
}

TYsonProducer CreateProducerForSchemafulDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("Schemaful DSV is supported only for tabular data");
    }
    auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseSchemafulDsv(input, consumer, config);
    });
}

TYsonProducer CreateProducerForJson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    auto ysonType = DataTypeToYsonType(dataType);
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseJson(input, consumer, config, ysonType);
    });
}

TYsonProducer CreateProducerForYson(EDataType dataType, IInputStream* input)
{
    auto ysonType = DataTypeToYsonType(dataType);
    return ConvertToProducer(TYsonInput(input, ysonType));
}

TYsonProducer CreateProducerForFormat(const TFormat& format, EDataType dataType, IInputStream* input)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateProducerForYson(dataType, input);
        case EFormatType::Json:
            return CreateProducerForJson(dataType, format.Attributes(), input);
        case EFormatType::Dsv:
            return CreateProducerForDsv(dataType, format.Attributes(), input);
        case EFormatType::Yamr:
            return CreateProducerForYamr(dataType, format.Attributes(), input);
        case EFormatType::YamredDsv:
            return CreateProducerForYamredDsv(dataType, format.Attributes(), input);
        case EFormatType::SchemafulDsv:
            return CreateProducerForSchemafulDsv(dataType, format.Attributes(), input);
        default:
            THROW_ERROR_EXCEPTION("Unsupported input format %Qlv",
                format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

template<class TBase>
struct TParserAdapter
    : public TBase
    , public IParser
{
public:
    template<class... TArgs>
    TParserAdapter(TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
    { }

    virtual void Read(TStringBuf data) override
    {
        TBase::Read(data);
    }

    virtual void Finish() override
    {
        TBase::Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForFormat(const TFormat& format, EDataType dataType, IYsonConsumer* consumer)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateParserForYson(consumer, DataTypeToYsonType(dataType));
        case EFormatType::Json: {
            auto config = ConvertTo<TJsonFormatConfigPtr>(&format.Attributes());
            return std::unique_ptr<IParser>(new TParserAdapter<TJsonParser>(consumer, config, DataTypeToYsonType(dataType)));
        }
        case EFormatType::Dsv: {
            auto config = ConvertTo<TDsvFormatConfigPtr>(&format.Attributes());
            return CreateParserForDsv(consumer, config);
        }
        case EFormatType::Yamr: {
            auto config = ConvertTo<TYamrFormatConfigPtr>(&format.Attributes());
            return CreateParserForYamr(consumer, config);
        }
        case EFormatType::YamredDsv: {
            auto config = ConvertTo<TYamredDsvFormatConfigPtr>(&format.Attributes());
            return CreateParserForYamredDsv(consumer, config);
        }
        case EFormatType::SchemafulDsv: {
            auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&format.Attributes());
            return CreateParserForSchemafulDsv(consumer, config);
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported input format %Qlv",
                format.GetType());
    }
}

std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    const std::vector<IValueConsumer*>& valueConsumers,
    int tableIndex)
{
    switch (format.GetType()) {
        case EFormatType::Protobuf: {
            auto config = ConvertTo<TProtobufFormatConfigPtr>(&format.Attributes());
            return CreateParserForProtobuf(valueConsumers[tableIndex], config, tableIndex);
        }
        case EFormatType::Skiff: {
            auto config = ConvertTo<TSkiffFormatConfigPtr>(&format.Attributes());
            return CreateParserForSkiff(valueConsumers[tableIndex], config, tableIndex);
        }
        default:
            return std::unique_ptr<IParser>(
                new TTableParserAdapter(format, valueConsumers, tableIndex));
    }
}

////////////////////////////////////////////////////////////////////////////////

void ConfigureEscapeTable(const TSchemafulDsvFormatConfigPtr& config, TEscapeTable* escapeTable)
{
    std::vector<char> stopSymbols = {config->RecordSeparator, config->FieldSeparator};
    if (config->EnableEscaping) {
        stopSymbols.push_back(config->EscapingSymbol);
        escapeTable->EscapingSymbol = config->EscapingSymbol;
    }
    escapeTable->FillStops(stopSymbols);
}

void ConfigureEscapeTables(
    const TDsvFormatConfigBasePtr& config,
    bool addCarriageReturn,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable)
{
    std::vector<char> stopSymbols = {config->RecordSeparator, config->FieldSeparator, '\0'};

    if (config->EnableEscaping) {
        stopSymbols.push_back(config->EscapingSymbol);
        keyEscapeTable->EscapingSymbol = valueEscapeTable->EscapingSymbol = config->EscapingSymbol;
    }

    if (addCarriageReturn) {
        stopSymbols.push_back('\r');
    }

    valueEscapeTable->FillStops(stopSymbols);

    stopSymbols.push_back(config->KeyValueSeparator);
    keyEscapeTable->FillStops(stopSymbols);
}

void ConfigureEscapeTables(
    const TYamrFormatConfigBasePtr& config,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    bool escapingForWriter,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable)
{
    std::vector<char> valueStopSymbols = {config->RecordSeparator};
    std::vector<char> keyStopSymbols = {config->RecordSeparator, config->FieldSeparator};

    if (enableKeyEscaping) {
        if (escapingForWriter) {
            keyStopSymbols.push_back('\0');
            keyStopSymbols.push_back('\r');
        }
        keyStopSymbols.push_back(config->EscapingSymbol);
        keyEscapeTable->EscapingSymbol = config->EscapingSymbol;
    }

    if (enableValueEscaping) {
        if (escapingForWriter) {
            valueStopSymbols.push_back('\0');
            valueStopSymbols.push_back('\r');
        }
        valueStopSymbols.push_back(config->EscapingSymbol);
        valueEscapeTable->EscapingSymbol = config->EscapingSymbol;
    }

    keyEscapeTable->FillStops(keyStopSymbols);
    valueEscapeTable->FillStops(valueStopSymbols);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
