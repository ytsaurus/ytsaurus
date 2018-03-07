#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A facade for locating chunks.
/*!
 *  Uploaded chunks can be registered either at TChunkStore or at TChunkCache.
 *  This class provides a single entry point for locating these chunks.
 *
 *  \note
 *  Thread affinity: any
 */
class TChunkRegistry
    : public TRefCounted
{
public:
    explicit TChunkRegistry(NCellNode::TBootstrap* bootstrap);

    //! Finds chunk by id. Returns |nullptr| if no chunk exists.
    IChunkPtr FindChunk(const TChunkId& chunkId);

    //! Finds chunk by id. Throws if no chunk exists.
    IChunkPtr GetChunkOrThrow(const TChunkId& chunkId);

private:
    NCellNode::TBootstrap* const Bootstrap_;

};

DEFINE_REFCOUNTED_TYPE(TChunkRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

