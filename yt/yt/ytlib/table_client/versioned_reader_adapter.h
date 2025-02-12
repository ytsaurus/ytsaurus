#pragma once

#include "public.h"

#include <yt/yt/client/table_client/versioned_reader.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

using TSchemafulReaderFactory = std::function<ISchemafulUnversionedReaderPtr(
    const TTableSchemaPtr&,
    const TColumnFilter&)
>;

IVersionedReaderPtr CreateVersionedReaderAdapter(
    TSchemafulReaderFactory createReader,
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateTimestampResettingAdapter(
    IVersionedReaderPtr underlyingReader,
    TTimestamp timestamp,
    NChunkClient::EChunkFormat chunkFormat);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
