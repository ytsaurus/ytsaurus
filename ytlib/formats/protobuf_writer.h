#pragma once

#include "public.h"
#include "config.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForProtobuf(
    TProtobufFormatConfigPtr config,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateSchemalessWriterForProtobuf(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
