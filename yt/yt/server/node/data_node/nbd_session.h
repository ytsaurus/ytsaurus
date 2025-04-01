#pragma once

#include "public.h"

#include "nbd_chunk_handler.h"
#include "location.h"
#include "session.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TNbdSession
    : public ISession
{
    TNbdSession(
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap,
        TSessionId sessionId,
        TSessionOptions options,
        TStoreLocationPtr storeLocation,
        NConcurrency::TLease lease,
        TLockedChunkGuard lockedChunkGuard);

    // NBD specific calls.

    //! Read size bytes from NBD chunk at offset.
    TFuture<NChunkClient::TBlock> Read(i64 offset, i64 size, ui64 cookie);

    //! Write buffer to NBD chunk at offset.
    TFuture<NIO::TIOCounters> Write(i64 offset, const NChunkClient::TBlock& block, ui64 cookie);

    //! Create NBD chunk and make filesystem on it.
    TFuture<void> Create();

    //! Remove NBD chunk and unregister NBD session.
    TFuture<void> Destroy();

    // Calls from base class.

    TChunkId GetChunkId() const& override;

    TSessionId GetId() const& override;

    ESessionType GetType() const override;

    NClusterNode::TMasterEpoch GetMasterEpoch() const override;

    const TWorkloadDescriptor& GetWorkloadDescriptor() const override;

    const TSessionOptions& GetSessionOptions() const override;

    const TStoreLocationPtr& GetStoreLocation() const override;

    TFuture<void> Start() override;

    void Cancel(const TError& error) override;

    TInstant GetStartTime() const override;
    i64 GetMemoryUsage() const override;
    i64 GetTotalSize() const override;
    i64 GetBlockCount() const override;
    i64 GetWindowSize() const override;
    i64 GetIntermediateEmptyBlockCount() const override;

    TFuture<TFinishResult> Finish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount,
        bool truncateExtraBlocks) override;

    bool ShouldUseProbePutBlocks() const override;
    void ProbePutBlocks(i64 requestedCumulativeMemorySize) override;
    i64 GetApprovedCumulativeBlockSize() const override;
    i64 GetMaxRequestedCumulativeBlockSize() const override;

    TFuture<NIO::TIOCounters> PutBlocks(
        int startBlockIndex,
        std::vector<NChunkClient::TBlock> blocks,
        i64 cumulativeBlockSize,
        bool enableCaching) override;

    TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> SendBlocks(
        int startBlockIndex,
        int blockCount,
        i64 cumulativeBlockSize,
        const NNodeTrackerClient::TNodeDescriptor& target) override;

    TFuture<TFlushBlocksResult> FlushBlocks(int blockIndex) override;

    void Ping() override;

    void OnUnregistered() override;

    void UnlockChunk() override;

    TFuture<void> GetUnregisteredEvent() override;

    DEFINE_SIGNAL_OVERRIDE(void(const TError& error), Finished);

private:
    const TDataNodeConfigPtr Config_;
    IBootstrap* Bootstrap_;
    const TSessionId Id_;
    const TSessionOptions Options_;
    const TStoreLocationPtr StoreLocation_;
    const NConcurrency::TLease Lease_;
    TLockedChunkGuard LockedChunkGuard_;
    const TInstant StartTime_;
    TPromise<void> UnregisteredEvent_ = NewPromise<void>();
    TError Error_;

    INbdChunkHandlerPtr NbdChunkHandler_;
};

DEFINE_REFCOUNTED_TYPE(TNbdSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
