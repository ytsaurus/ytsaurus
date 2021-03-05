#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader for a locally stored chunk.
/*!
 *  \note
 *  Thread affinity: any
 */
NChunkClient::IChunkReaderPtr CreateLocalChunkReader(
    NChunkClient::TReplicationReaderConfigPtr config,
    IChunkPtr chunk,
    IChunkBlockManagerPtr chunkBlockManager,
    NChunkClient::IBlockCachePtr blockCache,
    NTableClient::TBlockMetaCachePtr blockMetaCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

