#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>
#include <ytlib/ytree/attributes.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Type of data that can be read or written by a driver command.
DECLARE_ENUM(EDataType,
    (Null)
    (Binary)
    (Structured)
    (Tabular)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFormatType,
    (Null)
    (Yson)
    (Csv)
    (Dsv)
);

class TFormat
{
public:
    TFormat();
    TFormat(const TFormat& other);
    TFormat(EFormatType type, NYTree::IAttributeDictionary* attributes = NULL);

    TFormat& operator=(const TFormat& other);

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

TAutoPtr<NYTree::IParser> CreateParserForFormat(
    const TFormat& format,
    EDataType dataType,
    NYTree::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
