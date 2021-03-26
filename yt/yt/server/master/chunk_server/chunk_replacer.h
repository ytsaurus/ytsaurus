#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IChunkReplacerCallbacks
    : public virtual TRefCounted
{
    virtual void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) = 0;
    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) = 0;
    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd) = 0;
    
    virtual bool IsMutationLoggingEnabled() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReplacerCallbacks)

////////////////////////////////////////////////////////////////////////////////

class TChunkReplacer
{
public:
    explicit TChunkReplacer(
        IChunkReplacerCallbacksPtr chunkReplacerCallbacks,
        NLogging::TLogger logger = {});

    bool Replace(
        TChunkList* oldChunkList,
        TChunkList* newChunkList,
        TChunk* newChunk,
        const std::vector<TChunkId>& oldChunkIds);

private:
    struct TTraversalStateEntry
    {
        TChunkTree* ChunkTree;
        int Index;
    };

    const IChunkReplacerCallbacksPtr ChunkReplacerCallbacks_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
