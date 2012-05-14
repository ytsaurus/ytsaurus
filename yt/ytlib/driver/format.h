#pragma once

#include "public.h"
#include "command.h"

#include <ytlib/ytree/attributes.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFormatType,
    (Yson)
    (Csv)
);

class TFormat
{
public:
    TFormat(EFormatType type);

    EFormatType Type;

    TAutoPtr<NYTree::IAttributeDictionary> Attributes;

    static TFormat FromYson(NYTree::INodePtr node);
    void ToYson(NYTree::IYsonConsumer* consumer) const;
};

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<NYTree::IYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    TOutputStream* output);

NYTree::TYsonProducer CreateProducerForFormat(
    const TFormat& format,
    EDataType dataType,
    TInputStream* input);

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
