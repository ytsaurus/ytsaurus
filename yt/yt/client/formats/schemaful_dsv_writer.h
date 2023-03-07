#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "schemaless_writer_adapter.h"

#include <yt/client/table_client/unversioned_writer.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/blob.h>
#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/optional.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
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
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

NTableClient::IUnversionedRowsetWriterPtr CreateSchemafulWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr stream);

NTableClient::IUnversionedRowsetWriterPtr CreateSchemafulWriterForSchemafulDsv(
    const NYTree::IAttributeDictionary& attributes,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr stream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

