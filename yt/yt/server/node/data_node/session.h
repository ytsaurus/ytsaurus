#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/lib/nbd/chunk_block_device.h>

#include <yt/yt/server/node/data_node/location.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer_statistics.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/concurrency/lease_manager.h>

#include <optional>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TProbePutBlocksRequestSupplier
    : public TRefCounted
{
public:
    struct TRequest
    {
        i64 CumulativeBlockSize;
        TWorkloadDescriptor WorkloadDescriptor;

        std::strong_ordering operator<=>(const TRequest& other) const
        {
            return CumulativeBlockSize <=> other.CumulativeBlockSize;
        }
    };

public:
    explicit TProbePutBlocksRequestSupplier(TSessionId sessionId);

    TSessionId GetSessionId() const;

    void CancelRequests();
    bool IsCanceled() const;

    i64 GetCurrentApprovedMemory() const;
    i64 GetMaxRequestedMemory() const;

    std::optional<TRequest> GetMinRequest();
    void ApproveRequest(TLocationMemoryGuard&& memoryGuard, TRequest request);

    void PushRequest(TRequest request);

    void ReleaseResourcesForPutBlocks(i64 memory);

private:
    const TSessionId SessionId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::set<TRequest> Requests_;

    bool Canceled_ = false;

    i64 MaxRequestedMemory_ = 0;
    i64 ApprovedMemory_ = 0;

    TLocationMemoryGuard MemoryGuard_;
};

DEFINE_REFCOUNTED_TYPE(TProbePutBlocksRequestSupplier)

////////////////////////////////////////////////////////////////////////////////

struct TSessionOptions
{
    TWorkloadDescriptor WorkloadDescriptor;
    bool SyncOnClose = false;
    bool EnableMultiplexing = false;
    NChunkClient::TPlacementId PlacementId;
    bool DisableSendBlocks = false;
    bool UseProbePutBlocks = false;
    std::optional<i64> MinLocationAvailableSpace;
    std::optional<i64> NbdChunkSize;
    std::optional<NNbd::EFilesystemType> NbdChunkFsType;
    std::vector<std::pair<std::string, double>> FairShareTags;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a chunk upload session.
/*!
 *  \note
 *  Thread affinity: any
 */
struct ISession
    : public virtual TRefCounted
{
    struct TFinishResult
    {
        NChunkClient::NProto::TChunkInfo ChunkInfo;
        NChunkClient::TChunkWriterStatisticsPtr ChunkWriterStatistics;
    };

    struct TFlushBlocksResult
    {
        NIO::TIOCounters IOCounters;
        NChunkClient::TChunkWriterStatisticsPtr ChunkWriterStatistics;
    };

    struct TSendBlocksResult
    {
        bool NetThrottling;
        NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr TargetNodePutBlocksResult;
    };

    //! Returns the TChunkId being uploaded.
    virtual TChunkId GetChunkId() const& = 0;

    //! Returns the session ID.
    virtual TSessionId GetId() const& = 0;

    //! Returns the session type.
    virtual ESessionType GetType() const = 0;

    //! Returns the master epoch of session.
    virtual NClusterNode::TMasterEpoch GetMasterEpoch() const = 0;

    //! Returns the workload descriptor provided by the client during handshake.
    virtual const TWorkloadDescriptor& GetWorkloadDescriptor() const = 0;

    virtual const TSessionOptions& GetSessionOptions() const = 0;

    //! Returns the target chunk location.
    virtual const TStoreLocationPtr& GetStoreLocation() const = 0;

    //! Starts the session.
    /*!
     *  Returns the flag indicating that the session is persistently started.
     *  For blob chunks this happens immediately (and the actually opening happens in background).
     *  For journal chunks this happens when append record is flushed into the multiplexed changelog.
     */
    virtual TFuture<void> Start() = 0;

    //! Cancels the session.
    virtual void Cancel(const TError& error) = 0;

    virtual TInstant GetStartTime() const = 0;
    virtual i64 GetMemoryUsage() const = 0;
    virtual i64 GetTotalSize() const = 0;
    virtual i64 GetBlockCount() const = 0;
    virtual i64 GetWindowSize() const = 0;
    virtual i64 GetIntermediateEmptyBlockCount() const = 0;

    //! Finishes the session.
    virtual TFuture<TFinishResult> Finish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount,
        bool truncateExtraBlocks) = 0;

    //! Checks is probe put blocks should be used.
    virtual bool ShouldUseProbePutBlocks() const = 0;
    //! Prerequest memory for PutBlocks.
    virtual void ProbePutBlocks(i64 cumulativeBlockSize) = 0;
    virtual i64 GetApprovedCumulativeBlockSize() const = 0;
    virtual i64 GetMaxRequestedCumulativeBlockSize() const = 0;

    //! Puts a contiguous range of blocks into the window.
    virtual TFuture<NIO::TIOCounters> PutBlocks(
        int startBlockIndex,
        std::vector<NChunkClient::TBlock> blocks,
        i64 cumulativeBlockSize,
        bool enableCaching) = 0;

    //! Sends a range of blocks (from the current window) to another data node.
    virtual TFuture<TSendBlocksResult> SendBlocks(
        int startBlockIndex,
        int blockCount,
        i64 cumulativeBlockSize,
        TDuration requestTimeout,
        bool instantReplyOnThrottling,
        const NNodeTrackerClient::TNodeDescriptor& target) = 0;

    //! Flushes blocks up to a given one.
    virtual TFuture<TFlushBlocksResult> FlushBlocks(int blockIndex) = 0;

    //! Renews the lease.
    virtual void Ping() = 0;

    //! Called by session manager. Indicates that the session was unregistered.
    virtual void OnUnregistered() = 0;

    virtual void UnlockChunk() = 0;

    virtual TFuture<void> GetUnregisteredEvent() = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TError& error), Finished);
};

DEFINE_REFCOUNTED_TYPE(ISession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
