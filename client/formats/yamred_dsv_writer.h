#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "yamr_writer_base.h"

#include <yt/client/table_client/public.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/small_set.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamredDsv(
    TYamredDsvFormatConfigPtr config,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

