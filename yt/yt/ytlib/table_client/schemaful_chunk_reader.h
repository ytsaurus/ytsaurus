#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

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
    const TKeyColumns& keyColumns,
    const NChunkClient::TReadRange& readRange,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
