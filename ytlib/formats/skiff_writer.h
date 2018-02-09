#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/skiff/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForSkiff(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateSchemalessWriterForSkiff(
    const std::vector<NSkiff::TSkiffSchemaPtr>& tableSkiffSchemas,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormat
} // namespace NYT
