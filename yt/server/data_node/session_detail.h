#pragma once

#include "public.h"
#include "session.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <atomic>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public ISession
{
public:
    TSessionBase(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap,
        const TSessionId& sessionId,
        const TSessionOptions& options,
        TStoreLocationPtr location,
        NConcurrency::TLease lease);

    virtual const TChunkId& GetChunkId() const& override;
    virtual const TSessionId& GetId() const& override;
    virtual ESessionType GetType() const override;
    virtual const TWorkloadDescriptor& GetWorkloadDescriptor() const override;
    TStoreLocationPtr GetStoreLocation() const override;

    virtual TFuture<void> Start() override;

    virtual void Ping() override;

    virtual void Cancel(const TError& error) override;

    virtual TFuture<IChunkPtr> Finish(
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

    const IInvokerPtr WriteInvoker_;

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;

    bool Active_ = false;
    std::atomic<bool> Canceled_ = {false};

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    virtual TFuture<void> DoStart() = 0;
    virtual void DoCancel(const TError& error) = 0;
    virtual TFuture<IChunkPtr> DoFinish(
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

} // namespace NDataNode
} // namespace NYT

