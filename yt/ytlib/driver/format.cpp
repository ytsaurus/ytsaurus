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

TAutoPtr<IYsonConsumer> CreateConsumerForFormat(TFormat format, TOutputStream* output)
{
    if (format.Type == EFormat::Yson) {
        return CreateConsumerForYson(~format.Attributes, output);
    }
    YUNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateProducerForFormat(TFormat format, TInputStream* input)
{
    if (format.Type == EFormat::Yson) {
        return ProducerFromYson(input);
    }
    YUNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
