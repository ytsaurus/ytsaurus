#include "stdafx.h"
#include "format.h"

#include <ytlib/ytree/yson_writer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat(EFormatType type)
    : Type(type)
    , Attributes(EmptyAttributes())
{ }

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<IYsonConsumer> CreateConsumerForYson(
    IAttributeDictionary* attributes,
    TOutputStream* output)
{
    // TODO(panin): maybe parse via TYsonWriterConfig
    auto format = attributes->Get("format", EYsonFormat(EYsonFormat::Binary));
    auto type = attributes->Get<EYsonType>("type", EYsonType::Node);
    bool formatRaw = attributes->Get("raw", false);
    return new TYsonWriter(output, format, type, formatRaw);
}

TAutoPtr<IYsonConsumer> CreateConsumerForFormat(TFormat format, EDataType dataType, TOutputStream *output)
{
    switch (format.Type) {
        case EFormatType:
            return CreateConsumerForYson(~format.Attributes, output);
        default:
            YUNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateProducerForFormat(TFormat format, EDataType dataType, TInputStream *input)
{
    switch (format.Type) {
        case EFormatType:
            return ProducerFromYson(input);
        default:
            YUNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
