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
    TFormat(EFormatType type, NYTree::IAttributeDictionary* attributes = NULL);

    DEFINE_BYVAL_RO_PROPERTY(EFormatType, Type);
    NYTree::IAttributeDictionary* GetAttributes() const;

    static TFormat FromYson(NYTree::INodePtr node);
    void ToYson(NYTree::IYsonConsumer* consumer) const;

private:
    TAutoPtr<NYTree::IAttributeDictionary> Attributes;
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
