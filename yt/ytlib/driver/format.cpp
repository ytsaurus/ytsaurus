#include "stdafx.h"
#include "format.h"

#include <ytlib/ytree/yson_writer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat(EFormatType type, IAttributeDictionary* attributes)
    : Type_(type)
    , Attributes(attributes ? attributes->Clone() : CreateEphemeralAttributes())
{ }

TFormat TFormat::FromYson(INodePtr node)
{
    return TFormat(
        EFormatType::FromString(node->GetValue<Stroka>()),
        &node->Attributes());
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

TAutoPtr<IYsonConsumer> CreateConsumerForYson(
    IAttributeDictionary* attributes,
    TOutputStream* output)
{
    // TODO(panin): maybe parse via TYsonWriterConfig
    auto format = attributes->Get<EYsonFormat>("format", EYsonFormat::Binary);
    auto type = attributes->Get<EYsonType>("type", EYsonType::Node);
    bool formatRaw = attributes->Get("format_raw", false);
    return new TYsonWriter(output, format, type, formatRaw);
}

TAutoPtr<IYsonConsumer> CreateConsumerForFormat(const TFormat& format, EDataType dataType, TOutputStream* output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateConsumerForYson(format.GetAttributes(), output);
        default:
            YUNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateProducerForFormat(const TFormat& format, EDataType dataType, TInputStream* input)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return ProducerFromYson(input);
        default:
            YUNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
