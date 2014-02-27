#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Tries to create a reader for a locally stored chunk.
//! If the given chunk is not known, then returns |nullptr|.
/*! 
 *  \note
 *  Must be called from the Control Thread.
 */
NChunkClient::IAsyncReaderPtr CreateLocalChunkReader(
    NCellNode::TBootstrap* bootstrap,
    const NChunkClient::TChunkId& chunkId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

