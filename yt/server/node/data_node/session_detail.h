#pragma once

#include "public.h"
#include "session.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <atomic>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public ISession
{
public:
    TSessionBase(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap,
        TSessionId sessionId,
        const TSessionOptions& options,
        TStoreLocationPtr location,
        NConcurrency::TLease lease);

    virtual TChunkId GetChunkId() const& override;
    virtual TSessionId GetId() const& override;
    virtual ESessionType GetType() const override;
    virtual const TWorkloadDescriptor& GetWorkloadDescriptor() const override;
    const TStoreLocationPtr& GetStoreLocation() const override;

    virtual TFuture<void> Start() override;

    virtual void Ping() override;

    virtual void Cancel(const TError& error) override;

    virtual TFuture<NChunkClient::NProto::TChunkInfo> Finish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) override;

    virtual TFuture<void> PutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) override;

    virtual TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> SendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& targetDescriptor) override;

    virtual TFuture<void> FlushBlocks(int blockIndex) override;

    DEFINE_SIGNAL(void(const TError& error), Finished);

protected:
    const TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;
    const TSessionId SessionId_;
    const TSessionOptions Options_;
    const TStoreLocationPtr Location_;
    const NConcurrency::TLease Lease_;

    const IInvokerPtr SessionInvoker_;

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;

    // Affinity: session invoker.
    bool Active_ = false;
    TError PendingCancelationError_;
    
    std::atomic<bool> Canceled_ = false;


    virtual TFuture<void> DoStart() = 0;
    virtual void DoCancel(const TError& error) = 0;
    virtual TFuture<NChunkClient::NProto::TChunkInfo> DoFinish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) = 0;
    virtual TFuture<void> DoPutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) = 0;
    virtual TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> DoSendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) = 0;
    virtual TFuture<void> DoFlushBlocks(int blockIndex) = 0;

    void ValidateActive() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

