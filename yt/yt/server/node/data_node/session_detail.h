#pragma once

#include "session.h"
#include "location.h"

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

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
        TLockedChunkGuard lockedChunkGuard,
        NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions);

    TChunkId GetChunkId() const& override;
    TSessionId GetId() const& override;
    ESessionType GetType() const override;
    TInstant GetStartTime() const override;
    NClusterNode::TMasterEpoch GetMasterEpoch() const override;

    const TWorkloadDescriptor& GetWorkloadDescriptor() const override;
    const TStoreLocationPtr& GetStoreLocation() const override;
    const TSessionOptions& GetSessionOptions() const override;

    TFuture<void> Start() override;

    void Ping() override;

    void Cancel(const TError& error) override;

    TFuture<TFinishResult> Finish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount,
        bool truncateExtraBlocks) override;

    TFuture<NIO::TIOCounters> PutBlocks(
        int startBlockIndex,
        std::vector<NChunkClient::TBlock> blocks,
        i64 cumulativeBlockSize,
        bool enableCaching) override;

    TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> SendBlocks(
        int startBlockIndex,
        int blockCount,
        i64 cumulativeBlockSize,
        const NNodeTrackerClient::TNodeDescriptor& targetDescriptor) override;

    TFuture<TFlushBlocksResult> FlushBlocks(int blockIndex) override;

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
    const TInstant StartTime_;

    TLockedChunkGuard LockedChunkGuard_;
    const NChunkClient::IChunkWriter::TWriteBlocksOptions WriteBlocksOptions_;

    TPromise<void> UnregisteredEvent_ = NewPromise<void>();

    // Affinity: session invoker.
    bool Active_ = false;
    TError PendingCancelationError_;

    std::atomic<bool> Canceled_ = false;

    virtual TFuture<void> DoStart() = 0;
    virtual void DoCancel(const TError& error) = 0;
    virtual TFuture<TFinishResult> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount,
        bool truncateExtraBlocks) = 0;
    virtual TFuture<NIO::TIOCounters> DoPutBlocks(
        int startBlockIndex,
        std::vector<NChunkClient::TBlock> blocks,
        i64 cumulativeBlockSize,
        bool enableCaching) = 0;
    virtual TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        i64 cumulativeBlockSize,
        const NNodeTrackerClient::TNodeDescriptor& target) = 0;
    virtual TFuture<TFlushBlocksResult> DoFlushBlocks(int blockIndex) = 0;

    void ValidateActive() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
