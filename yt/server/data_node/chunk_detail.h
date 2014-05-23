#pragma once

#include "public.h"
#include "chunk.h"

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A base for any IChunk implementation.
class TChunk
    : public IChunk
{
public:
    virtual const TChunkId& GetId() const override;
    virtual TLocationPtr GetLocation() const override;
    virtual const NChunkClient::NProto::TChunkInfo& GetInfo() const override;
    virtual Stroka GetFileName() const override;

    virtual bool TryAcquireReadLock() override;
    virtual void ReleaseReadLock() override;
    virtual bool IsReadLockAcquired() const override;

    virtual TFuture<void> ScheduleRemoval() override;

protected:
    TChunkId Id_;
    TLocationPtr Location_;
    NChunkClient::NProto::TChunkInfo Info_;
    NCellNode::TNodeMemoryTracker* MemoryUsageTracker_;

    TRefCountedChunkMetaPtr Meta_;

    TSpinLock SpinLock_;
    TPromise<void> RemovedEvent_;
    int ReadLockCounter_ = 0;
    bool RemovalScheduled_ = false;

    TChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkInfo& info,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    ~TChunk();

    void DoRemove();
    virtual void EvictFromCache() = 0;
    virtual TFuture<void> RemoveFiles() = 0;

    TRefCountedChunkMetaPtr FilterCachedMeta(const std::vector<int>* tags) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

