#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader for a locally stored chunk.
/*!
 *  \note
 *  Thread affinity: any
 */
NChunkClient::IChunkReaderPtr CreateLocalChunkReader(
    NCellNode::TBootstrap* bootstrap,
    NChunkClient::TReplicationReaderConfigPtr config,
    IChunkPtr chunk,
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::IBlockCachePtr(),
    TClosure failureHandler = TClosure());

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

