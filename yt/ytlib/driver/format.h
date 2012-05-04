#pragma once

#include "public.h"
#include "command.h"

#include <ytlib/ytree/attributes.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFormatType,
    (Yson)
    (CSV)
);

struct TFormat
{
    TFormat(EFormatType type);

    EFormatType Type;
    TSharedPtr<NYTree::IAttributeDictionary> Attributes;
};

TAutoPtr<NYTree::IYsonConsumer> CreateConsumerForFormat(
    TFormat format,
    EDataType dataType,
    TOutputStream* output);

NYTree::TYsonProducer CreateProducerForFormat(
    TFormat format,
    EDataType dataType,
    TInputStream* input);

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
