#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/table_client/column_sort_schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Factory method, that creates a schemaful reader on top of any
//! NChunkClient::IReader, e.g. TMemoryReader, TReplicationReader etc.
ISchemafulUnversionedReaderPtr CreateSchemafulChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TTableSchemaPtr& resultSchema,
    const TSortColumns& sortColumns,
    const NChunkClient::TLegacyReadRange& readRange,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
