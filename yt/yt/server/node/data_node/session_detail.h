#pragma once

#include "session.h"
#include "location.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <atomic>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public ISession
{
public:
    TSessionBase(
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap,
        TSessionId sessionId,
        const TSessionOptions& options,
        TStoreLocationPtr location,
        NConcurrency::TLease lease,
        TLockedChunkGuard lockedChunkGuard);

    TChunkId GetChunkId() const& override;
    TSessionId GetId() const& override;
    ESessionType GetType() const override;
    NClusterNode::TMasterEpoch GetMasterEpoch() const override;

    const TWorkloadDescriptor& GetWorkloadDescriptor() const override;
    const TStoreLocationPtr& GetStoreLocation() const override;
    const TSessionOptions& GetSessionOptions() const override;

    TFuture<void> Start() override;

    void Ping() override;

    void Cancel(const TError& error) override;

    TFuture<NChunkClient::NProto::TChunkInfo> Finish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) override;

    TFuture<NIO::TIOCounters> PutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) override;

    TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> SendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& targetDescriptor) override;

    TFuture<NIO::TIOCounters> FlushBlocks(int blockIndex) override;

    void OnUnregistered() override;

    void UnlockChunk() override;

    TFuture<void> GetUnregisteredEvent() override;

    DEFINE_SIGNAL_OVERRIDE(void(const TError& error), Finished);

protected:
    const TDataNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;
    const TSessionId SessionId_;
    const TSessionOptions Options_;
    const TStoreLocationPtr Location_;
    const NConcurrency::TLease Lease_;
    const NClusterNode::TMasterEpoch MasterEpoch_;

    const IInvokerPtr SessionInvoker_;

    const NLogging::TLogger Logger;

    TLockedChunkGuard LockedChunkGuard_;

    TPromise<void> UnregisteredEvent_ = NewPromise<void>();

    // Affinity: session invoker.
    bool Active_ = false;
    TError PendingCancelationError_;

    std::atomic<bool> Canceled_ = false;


    virtual TFuture<void> DoStart() = 0;
    virtual void DoCancel(const TError& error) = 0;
    virtual TFuture<NChunkClient::NProto::TChunkInfo> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) = 0;
    virtual TFuture<NIO::TIOCounters> DoPutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) = 0;
    virtual TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) = 0;
    virtual TFuture<NIO::TIOCounters> DoFlushBlocks(int blockIndex) = 0;

    void ValidateActive() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

