#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/block_id.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/lock_free.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Class responsible for distribution of hot blocks to the peers.
class TP2PBlockDistributor
    : public TRefCounted
{
public:
    explicit TP2PBlockDistributor(NClusterNode::TBootstrap* bootstrap);

    //! Method that should be called on each block request.
    void OnBlockRequested(TBlockId blockId, i64 blockSize);

    //! Starts periodic activity.
    void Start();

private:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TP2PBlockDistributorConfigPtr Config_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    //! Blocks that were requested since last iteration of distribution. This stack
    //! is completely dequeued on each iteration and all information from it is accounted
    //! in `BlockRequestCount_` and `RequestHistory_`.
    TMultipleProducerSingleConsumerLockFreeStack<TBlockId> RecentlyRequestedBlocks_;

    //! History of what happened with block during the considered time window.
    struct TDistributionEntry
    {
        //! Number of times each block was accessed during the time window considered by the distributor
        //! (`Config_->WindowLength`).
        int RequestCount = 0;
        //! Last time this block was distributed (may be set to TInstant() if last distribution was earlier
        //! earlier than we started to track this block).
        TInstant LastDistributionTime;
        //! Number of times we distributed the block since we started tracking it.
        int DistributionCount = 0;
    };

    struct TChunkEntry
    {
        THashMap<int, TDistributionEntry> Blocks;
        THashSet<TNodeId> Nodes;
    };

    // NB: Two fields below are accessed only from `Invoker_`, so there are no races nor contention here.

    //! All necessary information about blocks that were at least once accessed during last `Config_->WindowLength`.
    THashMap<TChunkId, TChunkEntry> DistributionHistory_;

    //! At the beginning of each iteration requests that become obsolete (older than `Config_->WindowLength`)
    //! are swept out and corresponding entries in `BlockRequestCount_` are decremented.
    std::queue<std::pair<TInstant, TBlockId>> RequestHistory_;

    //! Total number of transmitted bytes over the selected network interfaces from the beginning of times
    //! (taken from /proc/net/dev/).
    i64 TransmittedBytes_ = 0;
    //! Total size of requested blocks from data node to the outworld during last `Config_->IterationPeriod` window.
    std::atomic<i64> TotalRequestedBlockSize_ = {0};
    //! Total number of bytes distributed up to this moment.
    std::atomic<i64> DistributedBytes_ = {0};

    NProfiling::TGauge OutTrafficGauge_;
    NProfiling::TGauge OutTrafficThrottlerQueueSizeGauge_;
    NProfiling::TGauge DefaultNetworkPendingOutBytesGauge_;
    NProfiling::TGauge TotalOutQueueSizeGauge_;
    NProfiling::TGauge TotalRequestedBlockSizeGauge_;

    void DoIteration();

    void SweepObsoleteRequests();
    void ProcessNewRequests();

    bool ShouldDistributeBlocks();

    void DistributeBlocks();

    struct TChosenBlocks
    {
        std::vector<NChunkClient::NProto::TReqPopulateCache> ReqTemplates;
        std::vector<NChunkClient::TBlock> Blocks;
        std::vector<TBlockId> BlockIds;
        i64 TotalSize = 0;
    };
    TChosenBlocks ChooseBlocks();

    std::vector<std::pair<NNodeTrackerClient::TNodeId, const NNodeTrackerClient::TNodeDescriptor*>> ChooseDestinationNodes(
        const THashMap<TNodeId, NNodeTrackerClient::TNodeDescriptor>& nodeSet,
        const std::vector<std::pair<NNodeTrackerClient::TNodeId, NNodeTrackerClient::TNodeDescriptor>>& nodes,
        const TCachedPeerListPtr& peerList,
        THashSet<NNodeTrackerClient::TNodeId>* preferredPeers) const;

    void UpdateTransmittedBytes();

    //! Method that registers the node as a peer for the given block
    //! as long as the node responds to the `PopulateCache` request.
    void OnBlockDistributed(
        const TString& address,
        const NNodeTrackerClient::TNodeId nodeId,
        const TBlockId& blockIds,
        i64 size,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspPopulateCachePtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TP2PBlockDistributor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
