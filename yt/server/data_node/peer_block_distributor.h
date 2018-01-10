#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/chunk_client/block_id.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/lock_free.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Class responsible for distribution of hot blocks to the peers.
class TPeerBlockDistributor
    : public TRefCounted
{
public:
    TPeerBlockDistributor(TPeerBlockDistributorConfigPtr config, NCellNode::TBootstrap* bootstrap);

    //! Method that should be called on each block request.
    void OnBlockRequested(TBlockId blockId, ui64 blockSize);

    //! Starts periodic activity.
    void Start();

private:
    const TPeerBlockDistributorConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;
    //! Serialized invoker in which all distribution iterations are performed.
    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    //! Blocks that were requested since last iteration of distribution. This stack
    //! is completely dequeued on each iteration and all information from it is accounted
    //! in `BlockRequestCount_` and `RequestHistory_`.
    TMultipleProducerSingleConsumerLockFreeStack<TBlockId> RecentlyRequestedBlocks_;

    //! History of what happened with block during the considered time window.
    struct TDistributionEntry {
        //! Number of times each block was accessed during the time window considered by the distributor
        //! (`Config_->WindowLength`).
        int RequestCount = 0;
        //! Last time this block was distributed (may be set to TInstant() if last distribution was earlier
        //! earlier than we started to track this block).
        TInstant LastDistributionTime;
        //! Number of times we distributed the block since we started tracking it.
        int DistributionCount = 0;
    };

    // NB: Two fields below are accessed only from `Invoker_`, so there are no races nor contention here.

    //! All necessary information about blocks that were at least once accessed during last `Config_->WindowLength`.
    yhash<TBlockId, TDistributionEntry> BlockIdToDistributionEntry_;
    //! At the beginning of each iteration requests that become obsolete (older than `Config_->WindowLength`)
    //! are swept out and corresponding entries in `BlockRequestCount_` are decremented.
    std::queue<std::pair<TInstant, TBlockId>> RequestHistory_;

    //! Total number of transmitted bytes over the selected network interfaces from the beginning of times
    //! (taken from /proc/net/dev/).
    ui64 TransmittedBytes_ = 0;
    //! Total size of requested blocks from data node to the outworld during last `Config_->Period` window.
    std::atomic<ui64> TotalRequestedBlockSize_ = {0};
    //! Total number of bytes distributed up to this moment.
    std::atomic<ui64> DistributedBytes_ = {0};

    void DoIteration();

    void SweepObsoleteRequests();
    void ProcessNewRequests();

    bool ShouldDistributeBlocks();

    void DistributeBlocks();

    struct TChosenBlocks
    {
        NChunkClient::NProto::TReqPopulateCache ReqTemplate;
        std::vector<NChunkClient::TBlock> Blocks;
        std::vector<TBlockId> BlockIds;
        ui64 BlockTotalSize = 0;
    };
    TChosenBlocks ChooseBlocks();

    std::vector<NNodeTrackerClient::TNodeDescriptor> ChooseDestinationNodes() const;

    void UpdateTransmittedBytes();

    //! Method that registers the node as a peer for all blocks chosen during one iteration
    //! as long as the node responds to the `PopulateCache` request.
    void OnBlocksDistributed(
        const TString& address,
        const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor,
        const std::vector<TBlockId>& blockIds,
        ui64 size,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspPopulateCachePtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TPeerBlockDistributor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
