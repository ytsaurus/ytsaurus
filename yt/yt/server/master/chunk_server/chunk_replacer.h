#pragma once

#include "public.h"
#include "private.h"

#include <stack>

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
    virtual TChunkList* CreateChunkList(
        EChunkListKind kind) = 0;

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

    bool FindChunkList(
        TChunkList* rootChunkList,
        TChunkListId desiredChunkListId);

    bool ReplaceChunkSequence(
        TChunk* newChunk,
        const std::vector<TChunkId>& oldChunkIds);

    TChunkList* Finish();

private:
    const IChunkReplacerCallbacksPtr ChunkReplacerCallbacks_;
    const NLogging::TLogger Logger;

    struct TTraversalStateEntry
    {
        TChunkTree* ChunkTree;
        int Index;
    };
    std::stack<TTraversalStateEntry> Stack_;
    int ChunkListIndex_ = 0;

    bool Initialized_ = false;

    TChunkList* PrevParentChunkList_ = nullptr;
    TChunkList* NewParentChunkList_ = nullptr;

    TChunkList* NewRootChunkList_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
