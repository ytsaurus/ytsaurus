#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaless_writer.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/property.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/attributes.h>
#include <yt/core/ytree/public.h>

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
    (SchemafulDsv)
    (Protobuf)
    (WebJson)
    (Skiff)
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
    : public NTableClient::ISchemalessWriter
{
    virtual TBlob GetContext() const = 0;

    virtual i64 GetWrittenSize() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessFormatWriter)

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& Format,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

NTableClient::IVersionedWriterPtr CreateVersionedWriterForFormat(
    const TFormat& Format,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForFormat(
    const TFormat& format,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IFlushableYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    IOutputStream* output);

NYson::TYsonProducer CreateProducerForFormat(
    const TFormat& format,
    EDataType dataType,
    IInputStream* input);

std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    EDataType dataType,
    NYson::IYsonConsumer* consumer);

std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    const std::vector<NTableClient::IValueConsumer*>& valueConsumers,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

void ConfigureEscapeTable(const TSchemafulDsvFormatConfigPtr& config, TEscapeTable* escapeTable);

void ConfigureEscapeTables(
    const TDsvFormatConfigBasePtr& config,
    bool addCarriageReturn,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable);

void ConfigureEscapeTables(
    const TYamrFormatConfigBasePtr& config,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    bool escapingForWriter,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
