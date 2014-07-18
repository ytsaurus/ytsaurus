#pragma once

#include "public.h"
#include "chunk.h"

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Chunk properties that can be obtained during the filesystem scan.
struct TChunkDescriptor
{
    TChunkId Id;
    NChunkClient::NProto::TChunkInfo Info;
};

//! A base for any IChunk implementation.
class TChunkBase
    : public IChunk
{
public:
    virtual const TChunkId& GetId() const override;
    virtual TLocationPtr GetLocation() const override;
    virtual const NChunkClient::NProto::TChunkInfo& GetInfo() const override;
    virtual Stroka GetFileName() const override;

    virtual int GetVersion() const override;
    virtual void IncrementVersion() override;

    virtual bool TryAcquireReadLock() override;
    virtual void ReleaseReadLock() override;
    virtual bool IsReadLockAcquired() const override;

    virtual TFuture<void> ScheduleRemove() override;

protected:
    NCellNode::TBootstrap* Bootstrap_;
    TLocationPtr Location_;
    TChunkId Id_;
    mutable NChunkClient::NProto::TChunkInfo Info_;

    int Version_ = 0;

    TRefCountedChunkMetaPtr Meta_;

    TSpinLock SpinLock_;
    TPromise<void> RemovedPromise_; // if not null then remove is scheduled
    int ReadLockCounter_ = 0;
    bool Removing_ = false;

    TChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkId& id,
        const NChunkClient::NProto::TChunkInfo& info);

    void StartAsyncRemove();
    virtual void EvictFromCache() = 0;
    virtual TFuture<void> AsyncRemove() = 0;

    TRefCountedChunkMetaPtr FilterCachedMeta(const std::vector<int>* tags) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

