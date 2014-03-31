#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/ytree/public.h>
#include <core/ytree/attributes.h>

#include <core/yson/public.h>

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
    (Json)
    (Dsv)
    (Yamr)
    (YamredDsv)
    (SchemedDsv)
    (SchemafulDsv)
);

class TFormat
{
public:
    TFormat();
    TFormat(const TFormat& other);
    TFormat(EFormatType type, const NYTree::IAttributeDictionary* attributes = nullptr);

    TFormat& operator = (const TFormat& other);

    DEFINE_BYVAL_RO_PROPERTY(EFormatType, Type);

    const NYTree::IAttributeDictionary& Attributes() const;

private:
    std::unique_ptr<NYTree::IAttributeDictionary> Attributes_;

};

void Serialize(const TFormat& value, NYson::IYsonConsumer* consumer);
void Deserialize(TFormat& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    TOutputStream* output);

NYTree::TYsonProducer CreateProducerForFormat(
    const TFormat& format,
    EDataType dataType,
    TInputStream* input);

std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    EDataType dataType,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
