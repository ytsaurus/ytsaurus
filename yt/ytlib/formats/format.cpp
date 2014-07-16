#include "stdafx.h"
#include "format.h"

#include "json_parser.h"
#include "json_writer.h"

#include "dsv_parser.h"
#include "dsv_writer.h"

#include "yamr_parser.h"
#include "yamr_writer.h"

#include "yamred_dsv_parser.h"
#include "yamred_dsv_writer.h"

#include "schemaful_dsv_parser.h"
#include "schemaful_dsv_writer.h"

#include "schemaless_writer_adapter.h"

#include "yson_parser.h"
#include "yson_writer.h"

#include <core/misc/error.h>

#include <core/yson/writer.h>

#include <core/ytree/fluent.h>
#include <core/ytree/forwarding_yson_consumer.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat()
    : Type_(EFormatType::Null)
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

    auto typeStr = node->GetValue<Stroka>();
    EFormatType type;
    try {
        type = ParseEnum<EFormatType>(typeStr);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Invalid format name %s",
            ~typeStr.Quote());
    }

    value = TFormat(type, &node->Attributes());
}

///////////////////////////////////////////////////////////////////////////////

EYsonType DataTypeToYsonType(EDataType dataType)
{
    switch (dataType) {
        case EDataType::Structured:
            return EYsonType::Node;
        case EDataType::Tabular:
            return EYsonType::ListFragment;
        default:
            THROW_ERROR_EXCEPTION("Data type %s is not supported by YSON",
                ~FormatEnum(dataType).Quote());
    }
}

std::unique_ptr<IYsonConsumer> CreateConsumerForYson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    TOutputStream* output)
{
    auto config = ConvertTo<TYsonFormatConfigPtr>(&attributes);
    auto ysonType = DataTypeToYsonType(dataType);
    auto enableRaw = (config->Format == EYsonFormat::Binary);
    
    return std::unique_ptr<IYsonConsumer>(new TYsonWriter(
        output,
        config->Format,
        ysonType,
        enableRaw,
        config->BooleanAsString));
}

std::unique_ptr<IYsonConsumer> CreateConsumerForJson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    TOutputStream* output)
{
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return CreateJsonConsumer(output, DataTypeToYsonType(dataType), config);
}

std::unique_ptr<IYsonConsumer> CreateConsumerForDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    TOutputStream* output)
{
    auto config = ConvertTo<TDsvFormatConfigPtr>(&attributes);
    switch (dataType) {
        case EDataType::Tabular:
            return std::unique_ptr<IYsonConsumer>(new TDsvTabularConsumer(output, config));

        case EDataType::Structured:
            return std::unique_ptr<IYsonConsumer>(new TDsvNodeConsumer(output, config));            

        case EDataType::Binary:
        case EDataType::Null:
            THROW_ERROR_EXCEPTION("Data type %s is not supported by DSV",
                ~FormatEnum(dataType).Quote());

        default:
            YUNREACHABLE();

    };
}

std::unique_ptr<IYsonConsumer> CreateConsumerForYamr(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    TOutputStream* output)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("YAMR is supported only for tabular data");
    }
    auto config = ConvertTo<TYamrFormatConfigPtr>(&attributes);
    return std::unique_ptr<IYsonConsumer>(new TYamrConsumer(output, config));
}

std::unique_ptr<IYsonConsumer> CreateConsumerForYamredDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    TOutputStream* output)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("Yamred DSV is supported only for tabular data");
    }
    auto config = ConvertTo<TYamredDsvFormatConfigPtr>(&attributes);
    return std::unique_ptr<IYsonConsumer>(new TYamredDsvConsumer(output, config));
}

std::unique_ptr<IYsonConsumer> CreateConsumerForSchemafulDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    TOutputStream* output)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("Schemaful DSV only is only supported for tabular data");
    }
    auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
    return std::unique_ptr<IYsonConsumer>(new TSchemafulDsvConsumer(output, config));
}

std::unique_ptr<IYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    TOutputStream* output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateConsumerForYson(dataType, format.Attributes(), output);
        case EFormatType::Json:
            return CreateConsumerForJson(dataType, format.Attributes(), output);
        case EFormatType::Dsv:
            return CreateConsumerForDsv(dataType, format.Attributes(), output);
        case EFormatType::Yamr:
            return CreateConsumerForYamr(dataType, format.Attributes(), output);
        case EFormatType::YamredDsv:
            return CreateConsumerForYamredDsv(dataType, format.Attributes(), output);
        // COMPAT(babenko): schemed -> schemaful
        case EFormatType::SchemedDsv:
        case EFormatType::SchemafulDsv:
            return CreateConsumerForSchemafulDsv(dataType, format.Attributes(), output);
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemafulWriterPtr CreateSchemafulWriterForYson(
    const IAttributeDictionary& attributes,
    IAsyncOutputStreamPtr output)
{
    auto config = ConvertTo<TYsonFormatConfigPtr>(&attributes);
    return New<TSchemafulYsonWriter>(output, config);
}

ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    const IAttributeDictionary& attributes,
    IAsyncOutputStreamPtr output)
{
    auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
    return New<TSchemafulDsvWriter>(output, config);
}

ISchemafulWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& format,
    IAsyncOutputStreamPtr output)
{
    switch (format.GetType()) {
        // TODO(babenko): schemaful
        case EFormatType::Yson:
            return CreateSchemafulWriterForYson(format.Attributes(), output);
        // TODO(babenko): schemaful
        case EFormatType::SchemafulDsv:
            return CreateSchemafulWriterForSchemafulDsv(format.Attributes(), output);
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr CreateSchemalessWriterAdaptor(
    std::unique_ptr<IYsonConsumer> consumer,
    TNameTablePtr nameTable)
{
    return New<TSchemalessWriterAdapter>(std::move(consumer), nameTable);
}

ISchemalessWriterPtr CreateSchemalessWriterForFormat(
    const TFormat& format,
    TNameTablePtr nameTable,
    TOutputStream* output)
{
    return CreateSchemalessWriterAdaptor(
        CreateConsumerForFormat(format, EDataType::Tabular, output),
        nameTable);
}

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateProducerForDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    TInputStream* input)
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
    TInputStream* input)
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
    TInputStream* input)
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
    TInputStream* input)
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
    TInputStream* input)
{
    auto ysonType = DataTypeToYsonType(dataType);
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseJson(input, consumer, config, ysonType);
    });
}

TYsonProducer CreateProducerForYson(EDataType dataType, TInputStream* input)
{
    auto ysonType = DataTypeToYsonType(dataType);
    return ConvertToProducer(TYsonInput(input, ysonType));
}

TYsonProducer CreateProducerForFormat(const TFormat& format, EDataType dataType, TInputStream* input)
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
            THROW_ERROR_EXCEPTION("Unsupported input format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForFormat(const TFormat& format, EDataType dataType, IYsonConsumer* consumer)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateParserForYson(consumer, DataTypeToYsonType(dataType));
        case EFormatType::Json: {
            auto config = ConvertTo<TJsonFormatConfigPtr>(&format.Attributes());
            return std::unique_ptr<IParser>(new TJsonParser(consumer, config, DataTypeToYsonType(dataType)));
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
        case EFormatType::SchemedDsv:
        case EFormatType::SchemafulDsv: {
            auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&format.Attributes());
            return CreateParserForSchemafulDsv(consumer, config);
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported input format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
