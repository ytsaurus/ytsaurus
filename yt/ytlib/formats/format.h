#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/ytree/public.h>
#include <core/ytree/attributes.h>

#include <core/yson/public.h>

#include <core/concurrency/public.h>

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/schemaless_writer.h>

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

struct ISchemalessFormatWriter
    : public NVersionedTableClient::ISchemalessWriter
{
    virtual void WriteTableIndex(int tableIndex) = 0;

    virtual void WriteRangeIndex(i32 rangeIndex) = 0;

    virtual void WriteRowIndex(i64 rowIndex) = 0;

    virtual TBlob GetContext() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessFormatWriter)

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    TOutputStream* output);

NVersionedTableClient::ISchemafulWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& Format,
    NConcurrency::IAsyncOutputStreamPtr output);

ISchemalessFormatWriterPtr CreateSchemalessWriterForFormat(
    const TFormat& format,
    NVersionedTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    bool enableKeySwitch,
    int keyColumnCount);

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
