#include "stdafx.h"
#include "format.h"

#include <ytlib/ytree/yson_writer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat(EFormatType type)
    : Type(type)
{ }

TFormat TFormat::FromYson(INodePtr node)
{
    TFormat format(EFormatType::FromString(node->GetValue<Stroka>()));
    format.Attributes = &node->Attributes();
    return format;
}

void TFormat::ToYson(IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
        .EndAttributes()
        .Scalar(Type.ToString())
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
    switch (format.Type) {
        case EFormatType::Yson:
            return CreateConsumerForYson(~format.Attributes, output);
        default:
            YUNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateProducerForFormat(const TFormat& format, EDataType dataType, TInputStream* input)
{
    switch (format.Type) {
        case EFormatType::Yson:
            return ProducerFromYson(input);
        default:
            YUNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
