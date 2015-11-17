#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/ytree/public.h>
#include <core/ytree/attributes.h>

#include <core/yson/public.h>

#include <core/concurrency/public.h>

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/schemaless_writer.h>

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
};

DEFINE_REFCOUNTED_TYPE(ISchemalessFormatWriter)

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& Format,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */);

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamr(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamredDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */);

ISchemalessFormatWriterPtr CreateSchemalessWriterForFormat(
    const TFormat& format,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    TOutputStream* output);

NYson::TYsonProducer CreateProducerForFormat(
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
