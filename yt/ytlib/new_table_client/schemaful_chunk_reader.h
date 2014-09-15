#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Factory method, that creates a schemaful reader on top of any
//! NChunkClient::IAsyncReader, e.g. TMemoryReader, TReplicationReader etc.
ISchemafulReaderPtr CreateSchemafulChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr uncompressedBlockCache,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const NChunkClient::TReadLimit& startLimit = NChunkClient::TReadLimit(),
    const NChunkClient::TReadLimit& endLimit = NChunkClient::TReadLimit(),
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
