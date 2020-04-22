#pragma once

#include "public.h"
#include "chunk.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/core/profiling/timing.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Chunk properties that can be obtained during the filesystem scan.
struct TChunkDescriptor
{
    TChunkDescriptor() = default;

    explicit TChunkDescriptor(TChunkId id)
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
    virtual TChunkId GetId() const override;
    virtual const TLocationPtr& GetLocation() const override;
    virtual TString GetFileName() const override;

    virtual int GetVersion() const override;
    virtual int IncrementVersion() override;

    virtual void AcquireReadLock() override;
    virtual void ReleaseReadLock() override;

    virtual void AcquireUpdateLock() override;
    virtual void ReleaseUpdateLock() override;

    virtual TFuture<void> ScheduleRemove() override;
    virtual bool IsRemoveScheduled() const override;

protected:
    NCellNode::TBootstrap* const Bootstrap_;
    const TLocationPtr Location_;
    const TChunkId Id_;

    std::atomic<int> Version_ = 0;

    TSpinLock SpinLock_;
    TFuture<void> RemovedFuture_;  // if not null then remove is scheduled
    TPromise<void> RemovedPromise_;
    int ReadLockCounter_ = 0;
    int UpdateLockCounter_ = 0;
    bool Removing_ = false;


    struct TReadSessionBase
        : public TIntrinsicRefCounted
    {
        NProfiling::TWallTimer SessionTimer;
        std::optional<TChunkReadGuard> ChunkReadGuard;
        TBlockReadOptions Options;
    };

    using TReadSessionBasePtr = TIntrusivePtr<TReadSessionBase>;


    struct TReadMetaSession
        : public TReadSessionBase
    { };

    using TReadMetaSessionPtr = TIntrusivePtr<TReadMetaSession>;


    TChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        TChunkId id);
    ~TChunkBase();
    
    void StartAsyncRemove();
    virtual TFuture<void> AsyncRemove() = 0;

    static NChunkClient::TRefCountedChunkMetaPtr FilterMeta(
        NChunkClient::TRefCountedChunkMetaPtr meta,
        const std::optional<std::vector<int>>& extensionTags);

    void StartReadSession(
        const TReadSessionBasePtr& session,
        const TBlockReadOptions& options);
    void ProfileReadBlockSetLatency(const TReadSessionBasePtr& session);
    void ProfileReadMetaLatency(const TReadSessionBasePtr& session);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

