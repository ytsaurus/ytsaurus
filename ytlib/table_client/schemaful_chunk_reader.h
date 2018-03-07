#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Factory method, that creates a schemaful reader on top of any
//! NChunkClient::IReader, e.g. TMemoryReader, TReplicationReader etc.
ISchemafulReaderPtr CreateSchemafulChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    const NChunkClient::TReadSessionId& sessionId,
    const TTableSchema& resultSchema,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const NChunkClient::TReadRange& readRange,
    TTimestamp timestamp = NullTimestamp);

ISchemafulReaderPtr CreateSchemafulChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    const NChunkClient::TReadSessionId& sessionId,
    const TTableSchema& resultSchema,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
