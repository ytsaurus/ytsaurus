#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/actions/callback.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader for a locally stored chunk.
/*!
 *  \note
 *  Thread affinity: any
 */
NChunkClient::IChunkReaderPtr CreateLocalChunkReader(
    NChunkClient::TReplicationReaderConfigPtr config,
    IChunkPtr chunk,
    TChunkBlockManagerPtr chunkBlockManager,
    NChunkClient::IBlockCachePtr blockCache,
    TClosure failureHandler = TClosure());

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

