#pragma once

#include "public.h"
#include "chunk.h"

#include <yt/server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Chunk properties that can be obtained during the filesystem scan.
struct TChunkDescriptor
{
    TChunkDescriptor()
    { }

    explicit TChunkDescriptor(const TChunkId& id)
        : Id(id)
    { }

    TChunkId Id;
    i64 DiskSpace = 0;

    // For journal chunks only.
    i64 RowCount = 0;
    bool Sealed = false;
};

////////////////////////////////////////////////////////////////////////////////

//! A base for any IChunk implementation.
class TChunkBase
    : public IChunk
{
public:
    virtual const TChunkId& GetId() const override;
    virtual TLocationPtr GetLocation() const override;
    virtual TString GetFileName() const override;

    virtual int GetVersion() const override;
    virtual void IncrementVersion() override;

    virtual bool IsAlive() const override;
    virtual void SetDead() override;

    virtual bool TryAcquireReadLock() override;
    virtual void ReleaseReadLock() override;
    virtual bool IsReadLockAcquired() const override;

    virtual TFuture<void> ScheduleRemove() override;
    virtual bool IsRemoveScheduled() const override;

protected:
    NCellNode::TBootstrap* const Bootstrap_;
    const TLocationPtr Location_;
    const TChunkId Id_;

    int Version_ = 0;

    std::atomic<bool> Alive_ = {true};

    TSpinLock SpinLock_;
    TFuture<void> RemovedFuture_;  // if not null then remove is scheduled
    TPromise<void> RemovedPromise_;
    int ReadLockCounter_ = 0;
    bool Removing_ = false;


    TChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkId& id);

    void StartAsyncRemove();
    virtual TFuture<void> AsyncRemove() = 0;

    static NChunkClient::TRefCountedChunkMetaPtr FilterMeta(
        NChunkClient::TRefCountedChunkMetaPtr meta,
        const std::optional<std::vector<int>>& extensionTags);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

