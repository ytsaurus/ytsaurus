#pragma once

#include "public.h"

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A facade for locating chunks that are fully uploaded to the chunk holder.
/*!
 *  Uploaded chunks can be registered either at TChunkStore or at TChunkCache.
 *  This class provides a single entry point for locating these chunks.
 */
class TChunkRegistry
    : public TRefCounted
{
public:
    explicit TChunkRegistry(NCellNode::TBootstrap* bootstrap);

    //! Finds chunk by id. Returns |nullptr| if no chunk exists.
    TChunkPtr FindChunk(const TChunkId& chunkId) const;

private:
    NCellNode::TBootstrap* Bootstrap_;

};

DEFINE_REFCOUNTED_TYPE(TChunkRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

