#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/column_sort_schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Factory method, that creates a schemaful reader on top of any
//! NChunkClient::IReader, e.g. TMemoryReader, TReplicationReader etc.
ISchemafulUnversionedReaderPtr CreateSchemafulChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TTableSchemaPtr& resultSchema,
    const TSortColumns& sortColumns,
    const NChunkClient::TReadRange& readRange);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
