#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Factory method, that creates a schemaful reader on top of any
//! NChunkClient::IReader, e.g. TMemoryReader, TReplicationReader etc.
ISchemafulReaderPtr CreateSchemafulChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TTableSchema& resultSchema,
    const TKeyColumns& keyColumns,
    const NChunkClient::TReadRange& readRange,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
