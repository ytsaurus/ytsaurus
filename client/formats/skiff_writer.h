#pragma once

#include "public.h"

#include <yt/client/table_client/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/library/skiff/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchema>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const std::vector<NSkiff::TSkiffSchemaPtr>& tableSkiffSchemas,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchema>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormat
