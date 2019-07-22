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

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    TWebJsonFormatConfigPtr config,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchema>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output);

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchema>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
