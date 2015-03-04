#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/ytree/public.h>
#include <core/ytree/attributes.h>

#include <core/yson/public.h>

#include <core/concurrency/public.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Type of data that can be read or written by a driver command.
DEFINE_ENUM(EDataType,
    (Null)
    (Binary)
    (Structured)
    (Tabular)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFormatType,
    (Null)
    (Yson)
    (Json)
    (Dsv)
    (Yamr)
    (YamredDsv)
    // COMPAT(babenko): schemed -> schemaful
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

NVersionedTableClient::ISchemafulWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& format,
    NConcurrency::IAsyncOutputStreamPtr output);

NVersionedTableClient::ISchemalessWriterPtr CreateSchemalessWriterAdapter(
    std::unique_ptr<NYson::IYsonConsumer> consumer,
    NVersionedTableClient::TNameTablePtr nameTable);

NVersionedTableClient::ISchemalessWriterPtr CreateSchemalessWriterForFormat(
    const TFormat& format,
    NVersionedTableClient::TNameTablePtr nameTable,
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
