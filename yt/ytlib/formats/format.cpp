#include "stdafx.h"
#include "format.h"

#include "tsv_parser.h"
#include "tsv_writer.h"

#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat()
    : Type_(EFormatType::Null)
{ }

TFormat::TFormat(EFormatType type, IAttributeDictionary* attributes)
    : Type_(type)
    , Attributes(attributes ? attributes->Clone() : CreateEphemeralAttributes())
{ }

TFormat::TFormat(const TFormat& other)
    : Type_(other.Type_)
    , Attributes(other.Attributes->Clone())
{ }

TFormat& TFormat::operator=(const TFormat& other)
{
    if (this != &other) {
        Attributes = other.Attributes->Clone();
        Type_ = other.Type_;
    }

    return *this;
}

TFormat TFormat::FromYson(INodePtr node)
{
    if (node->GetType() != ENodeType::String) {
        ythrow yexception() << "Format must be a string";
    }

    auto typeStr = node->GetValue<Stroka>();
    EFormatType type;
    try {
        type = ParseEnum<EFormatType>(typeStr);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Invalid format type %s",
            ~typeStr.Quote());
    }

    return TFormat(type, &node->Attributes());
}

void TFormat::ToYson(IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Items(~Attributes)
        .EndAttributes()
        .Scalar(Type_.ToString());
}

IAttributeDictionary* TFormat::GetAttributes() const
{
    return ~Attributes;
}

////////////////////////////////////////////////////////////////////////////////

EYsonType DataTypeToYsonType(EDataType dataType)
{
    switch (dataType) {
        case EDataType::Structured:
            return EYsonType::Node;
        case EDataType::Tabular:
            return EYsonType::ListFragment;
        default:
            ythrow yexception() << Sprintf("Data type %s is not supported by YSON",
                ~FormatEnum(dataType).Quote());
    }
}

TAutoPtr<IYsonConsumer> CreateConsumerForYson(
    EDataType dataType,
    IAttributeDictionary* attributes,
    TOutputStream* output)
{
    class TNewlineAppendingConsumer
        : public TForwardingYsonConsumer
    {
    public:
        explicit TNewlineAppendingConsumer(
            TOutputStream* output,
            TAutoPtr<IYsonConsumer> underlyingConsumer,
            EYsonType ysonType)
            : Output(output)
            , UnderlyingConsumer(underlyingConsumer)
        {
            Forward(
                ~UnderlyingConsumer,
                BIND(&TNewlineAppendingConsumer::OnFinished, this),
                ysonType);
        }

    private:
        TOutputStream* Output;
        TAutoPtr<IYsonConsumer> UnderlyingConsumer;

        void OnFinished()
        {
            Output->Write('\n');
        }
    };

    try {
        auto ysonFormat = attributes->Find<EYsonFormat>("format");
        auto ysonType = DataTypeToYsonType(dataType);
        auto enableRaw = attributes->Find<bool>("enable_raw");
        if (!ysonFormat) {
            ysonFormat = EYsonFormat::Binary;
            enableRaw = true;
        } else {
            if (!enableRaw) {
                // In case of textual format we would like to force textual output.
                enableRaw = (*ysonFormat == EYsonFormat::Binary);
            }
        }

        TAutoPtr<IYsonConsumer> writer(new TYsonWriter(output, *ysonFormat, ysonType, *enableRaw));
        return *ysonFormat == EYsonFormat::Binary
            ? writer
            : new TNewlineAppendingConsumer(output, writer, ysonType);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing YSON output format\n", ex.what());
    }
}

TAutoPtr<IYsonConsumer> CreateConsumerForTsv(
    EDataType dataType,
    IAttributeDictionary* attributes,
    TOutputStream* output)
{
    // TODO(panin): use attributes, luke!
    return new TTsvWriter(output);
}

TAutoPtr<IYsonConsumer> CreateConsumerForFormat(const TFormat& format, EDataType dataType, TOutputStream* output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateConsumerForYson(dataType, format.GetAttributes(), output);
        case EFormatType::Tsv:
            return CreateConsumerForTsv(dataType, format.GetAttributes(), output);
        default:
            ythrow yexception() << Sprintf("Unsupported output format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
}

TYsonProducer CreateProducerForTsv(EDataType dataType, TInputStream* input)
{
    return BIND([=] (IYsonConsumer* consumer) {
        ParseTsv(input, consumer);
    });
}

TYsonProducer CreateProducerForYson(EDataType dataType, TInputStream* input)
{
    auto ysonType = DataTypeToYsonType(dataType);
    return ProducerFromYson(input, ysonType);
}

TYsonProducer CreateProducerForFormat(const TFormat& format, EDataType dataType, TInputStream* input)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateProducerForYson(dataType, input);
        case EFormatType::Tsv:
            return CreateProducerForTsv(dataType, input);
        default:
            ythrow yexception() << Sprintf("Unsupported input format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
}

TAutoPtr<NYTree::IParser> CreateParserForFormat(const TFormat& format, EDataType dataType, NYTree::IYsonConsumer* consumer)
{
    switch (format.GetType()) {
    case EFormatType::Yson:
        return new TYsonParser(consumer, DataTypeToYsonType(dataType));
    case EFormatType::Tsv:
        // ToDo(psushin): use config from format.
        return new TTsvParser(consumer);
    default:
        ythrow yexception() << Sprintf("Unsupported input format %s",
            ~FormatEnum(format.GetType()).Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
