#include "stdafx.h"
#include "format.h"

#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat()
    : Type_(EFormatType::Null)
{ }

TFormat::TFormat(EFormatType type, IAttributeDictionary* attributes)
    : Type_(type)
    , Attributes(attributes ? attributes->Clone() : CreateEphemeralAttributes())
{ }

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

IAttributeDictionary* NYT::NDriver::TFormat::GetAttributes() const
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
        auto ysonFormat = attributes->Get<EYsonFormat>("format", EYsonFormat::Binary);
        auto ysonType = DataTypeToYsonType(dataType);
        bool enableRaw = attributes->Get("enable_raw", true);
        TAutoPtr<IYsonConsumer> writer(new TYsonWriter(output, ysonFormat, ysonType, enableRaw));
        return ysonFormat == EYsonFormat::Binary
            ? writer
            : new TNewlineAppendingConsumer(output, writer, ysonType);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing YSON output format\n", ex.what());
    }
}

TAutoPtr<IYsonConsumer> CreateConsumerForFormat(const TFormat& format, EDataType dataType, TOutputStream* output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateConsumerForYson(dataType, format.GetAttributes(), output);

        default:
            ythrow yexception() << Sprintf("Unsupported output format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
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

        default:
            ythrow yexception() << Sprintf("Unsupported input format %s",
                ~FormatEnum(format.GetType()).Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
