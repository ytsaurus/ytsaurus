#include "peer_block_distributor.h"

#include "peer_block_table.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/config.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/misc/proc.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/retrying_channel.h>
#include <yt/core/rpc/dispatcher.h>

#include <yt/core/bus/tcp_dispatcher.h>

#include <util/random/random.h>

namespace NYT {
namespace NDataNode {

static const auto& Logger = P2PLogger;
static const auto& Profiler = P2PProfiler;

using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NBus;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TPeerBlockDistributor::TPeerBlockDistributor(TPeerBlockDistributorConfigPtr config, TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , Invoker_(CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TPeerBlockDistributor::DoIteration, MakeWeak(this)),
        Config_->Period))
{ }

void TPeerBlockDistributor::OnBlockRequested(TBlockId blockId, ui64 blockSize)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TotalRequestedBlockSize_ += blockSize;
    RecentlyRequestedBlocks_.Enqueue(blockId);
}

void TPeerBlockDistributor::Start()
{
    UpdateTransmittedBytes();
    PeriodicExecutor_->Start();
}

void TPeerBlockDistributor::DoIteration()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    ProcessNewRequests();
    SweepObsoleteRequests();

    if (ShouldDistributeBlocks()) {
        ui64 distributedBlockSize = DistributeBlocks();
        Profiler.Enqueue("/distributed_block_size", distributedBlockSize, EMetricType::Gauge);
    }
}

void TPeerBlockDistributor::SweepObsoleteRequests()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto now = TInstant::Now();
    while (!RequestHistory_.empty()) {
        TBlockId blockId;
        TInstant requestTime;
        std::tie(requestTime, blockId) = RequestHistory_.front();
        if (requestTime + Config_->WindowLength <= now) {
            auto it = BlockIdToDistributionEntry_.find(blockId);
            YCHECK(it != BlockIdToDistributionEntry_.end());
            if (--it->second.RequestCount == 0) {
                BlockIdToDistributionEntry_.erase(it);
            }
            RequestHistory_.pop();
        } else {
            break;
        }
    }
}

void TPeerBlockDistributor::ProcessNewRequests()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto now = TInstant::Now();

    RecentlyRequestedBlocks_.DequeueAll(true /* reversed */, [&] (const TBlockId& blockId) {
        RequestHistory_.push(std::make_pair(now, blockId));
        ++BlockIdToDistributionEntry_[blockId].RequestCount;
    });
}

bool TPeerBlockDistributor::ShouldDistributeBlocks()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    ui64 oldTransmittedBytes = TransmittedBytes_;
    UpdateTransmittedBytes();
    ui64 outTraffic = TransmittedBytes_ - oldTransmittedBytes;

    ui64 outThrottlerQueueSize = Bootstrap_->GetOutThrottler(TWorkloadDescriptor())->GetQueueTotalCount();
    ui64 defaultNetworkPendingOutBytes = 0;
    if (auto defaultNetwork = Bootstrap_->GetDefaultNetworkName()) {
        defaultNetworkPendingOutBytes = TTcpDispatcher::Get()->GetCounters(*defaultNetwork)->PendingOutBytes;
    }
    ui64 totalOutQueueSize = outThrottlerQueueSize + defaultNetworkPendingOutBytes;

    ui64 totalRequestedBlockSize = TotalRequestedBlockSize_;

    bool shouldDistributeBlocks =
        outTraffic > Config_->OutTrafficActivationThreshold ||
        totalOutQueueSize > Config_->OutQueueSizeActivationThreshold ||
        totalRequestedBlockSize > Config_->TotalRequestedBlockSizeActivationThreshold;

    LOG_INFO("Determining if blocks should be distributed (Period: %v, OutTraffic: %v, "
        "OutTrafficActivationThreshold: %v, OutThrottlerQueueSize: %v, DefaultNetworkPendingOutBytes: %v, "
        "TotalOutQueueSize: %v, OutQueueSizeActivationThreshold: %v, TotalRequestedBlockSize: %v, "
        "TotalRequestedBlockSizeActivationThreshold: %v, ShouldDistributeBlocks: %v)",
        Config_->Period,
        outTraffic,
        Config_->OutTrafficActivationThreshold,
        outThrottlerQueueSize,
        defaultNetworkPendingOutBytes,
        totalOutQueueSize,
        Config_->OutQueueSizeActivationThreshold,
        totalRequestedBlockSize,
        Config_->TotalRequestedBlockSizeActivationThreshold,
        shouldDistributeBlocks);

    // Do not forget to reset the requested block size for the next iteration.
    TotalRequestedBlockSize_ = 0;

    // Carefully profile all related values.
    Profiler.Enqueue("/out_traffic", outTraffic, EMetricType::Gauge);
    Profiler.Enqueue("/out_throttler_queue_size", outThrottlerQueueSize, EMetricType::Gauge);
    Profiler.Enqueue("/default_network_pending_out_bytes", defaultNetworkPendingOutBytes, EMetricType::Gauge);
    Profiler.Enqueue("/total_out_queue_size", totalOutQueueSize, EMetricType::Gauge);
    Profiler.Enqueue("/total_requested_block_size", totalRequestedBlockSize, EMetricType::Gauge);

    return shouldDistributeBlocks;
}

ui64 TPeerBlockDistributor::DistributeBlocks()
{
    auto chosenBlocks = ChooseBlocks();
    TReqPopulateCache reqTemplate = std::move(chosenBlocks.ReqTemplate);
    std::vector<TBlock> blocks = std::move(chosenBlocks.Blocks);
    std::vector<TBlockId> blockIds = std::move(chosenBlocks.BlockIds);
    auto totalBlockSize = chosenBlocks.BlockTotalSize;

    if (blocks.empty()) {
        LOG_INFO("No blocks may be distributed on current iteration");
        return 0;
    }

    auto destinationNodes = ChooseDestinationNodes();

    if (destinationNodes.empty()) {
        LOG_INFO("No suitable destination nodes found");
    }

    LOG_INFO("Ready to distribute blocks (BlockCount: %v, TotalBlockSize: %v, DestinationNodes: %v)",
        blocks.size(),
        totalBlockSize,
        destinationNodes);

    auto now = TInstant::Now();
    for (const auto& blockId : blockIds) {
        BlockIdToDistributionEntry_[blockId].LastDistributionTime = now;
    }

    const auto& channelFactory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetChannelFactory();
    
    for (const auto& destinationNode : destinationNodes) {
        const auto& destinationAddress = destinationNode.GetAddress(Bootstrap_->GetLocalNetworks());
        auto heavyChannel = CreateRetryingChannel(
            Config_->NodeChannel,
            channelFactory->CreateChannel(destinationAddress));
        TDataNodeServiceProxy proxy(std::move(heavyChannel));
        auto req = proxy.PopulateCache();
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        req->MergeFrom(reqTemplate);
        SetRpcAttachedBlocks(req, blocks);
        req->Invoke().Subscribe(BIND(
            &TPeerBlockDistributor::OnBlocksDistributed,
            MakeWeak(this),
            destinationAddress,
            destinationNode,
            blockIds));
    }
    return totalBlockSize;
}

TPeerBlockDistributor::TChosenBlocks TPeerBlockDistributor::ChooseBlocks()
{
    // First we filter the blocks requested during the considered window (`Config->WindowLength` from now) such that:
    // 1) Block was not recently distributed (within `Config_->ConsecutiveDistributionDelay` from now);
    // 2) Block does not have many peers (at most `Config_->BlockPeerCountLimit`);
    // These candidate blocks are sorted in a descending order of request count.
    // We iterate over the blocks forming a PopulateCache request of total size no more than
    // `Config_->MaxPopulateRequestSize`, and finally deliver it to no more than
    // `Config_->DestinationNodeCountPerIteration` nodes, marking them as peers to the processed blocks.

    auto now = TInstant::Now();

    struct TBlockCandidate {
        TBlockId BlockId;
        TInstant LastDistributionTime;
        int PeerCount;
        int RequestCount;

        bool operator <(const TBlockCandidate& other)
        {
            if (RequestCount != other.RequestCount) {
                return RequestCount > other.RequestCount;
            }
            if (PeerCount != other.PeerCount) {
                return PeerCount < other.PeerCount;
            }
            return false;
        }
    };

    // NB: we need to get peer counts for each candidate block
    // from the peer block table which may be accessed only
    // from the control thread.
    auto blockIdToPeerCount = WaitFor(
        BIND([=] {
			yhash<TBlockId, int> blockIdToPeerCount;
			for (const auto& pair : BlockIdToDistributionEntry_) {
				const auto& blockId = pair.first;
				int peerCount = Bootstrap_->GetPeerBlockTable()->GetPeers(blockId).size();
				blockIdToPeerCount[blockId] = peerCount;
			}
			return blockIdToPeerCount;
		})
			.AsyncVia(Bootstrap_->GetControlInvoker())
            .Run()
    ).ValueOrThrow();

    std::vector<TBlockCandidate> candidates;
    for (const auto& pair : BlockIdToDistributionEntry_) {
        const auto& blockId = pair.first;
        const auto& distributionEntry = pair.second;
        int peerCount = blockIdToPeerCount[blockId];
        YCHECK(distributionEntry.RequestCount > 0);
        if (distributionEntry.LastDistributionTime + Config_->ConsecutiveDistributionDelay <= now &&
            peerCount <= Config_->BlockPeerCountLimit)
        {
            candidates.emplace_back(TBlockCandidate{
                blockId,
                distributionEntry.LastDistributionTime,
                peerCount,
                distributionEntry.RequestCount});
        }
    }

    std::sort(candidates.begin(), candidates.end());

    TReqPopulateCache reqTemplate;
    std::vector<TBlock> blocks;
    std::vector<TBlockId> blockIds;
    ui64 totalSize = 0;

    const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();

    for (const auto& candidate : candidates) {
        auto blockId = candidate.BlockId;
        auto cachedBlock = chunkBlockManager->FindCachedBlock(blockId);
        if (!cachedBlock) {
            // TODO(max42): the block is both hot enough to be distributed,
            // but missing in the block cache? Sounds strange, but maybe we
            // should fetch it from the disk then?
            continue;
        }

        int requestCount = candidate.RequestCount;
        auto lastDistributionTime = candidate.LastDistributionTime;
        int peerCount = candidate.PeerCount;
        ui64 blockSize = cachedBlock->GetData().Size();
        auto source = cachedBlock->Source();
        auto block = cachedBlock->GetData();
        if (!source) {
            // TODO(max42): seems like the idea of remembering the source of a block
            // is currently not working properly (it is almost always Null) as there
            // are no calls of IBlockCache::Put with non-Null fourth argument except
            // in the replication reader.
            // I'm trying to deal with it assuming that the origin of a block with
            // Null source is current node.
            source = Bootstrap_->GetMasterConnector()->GetLocalDescriptor();
        }
        if (totalSize + blockSize <= Config_->MaxPopulateRequestSize ||
            Y_UNLIKELY(totalSize == 0)) // Force at least one block to be distributed even if it is huge.
        {
            LOG_DEBUG("Block is ready for distribution (BlockId: %v, RequestCount: %v, LastDistributionTime: %v, "
                "PeerCount: %v, Source: %v, Size: %v)",
                blockId,
                requestCount,
                lastDistributionTime,
                peerCount,
                source,
                blockSize);
            auto* protoBlock = reqTemplate.add_blocks();
            ToProto(protoBlock->mutable_block_id(), blockId);
            if (source) {
                ToProto(protoBlock->mutable_source_descriptor(), *source);
            }
            blocks.emplace_back(std::move(block));
            blockIds.emplace_back(blockId);
            totalSize += blockSize;
        }
    }

    return {std::move(reqTemplate), std::move(blocks), std::move(blockIds), totalSize};
}

std::vector<TNodeDescriptor> TPeerBlockDistributor::ChooseDestinationNodes() const
{
    std::vector<std::pair<double, TNodeDescriptor>> randomlyKeyedNodeDescriptors;

    auto localDescriptor = Bootstrap_->GetMasterConnector()->GetLocalDescriptor();

    struct TNodeCandidate
    {
        double RandomKey;
        TNodeDescriptor descriptor;

        bool operator <(const TNodeCandidate& other)
        {
            return RandomKey < other.RandomKey;
        }
    };

    std::vector<TNodeCandidate> candidates;

    for (const auto& nodeDescriptor : Bootstrap_->GetNodeDirectory()->GetAllDescriptors()) {
        if (nodeDescriptor != localDescriptor) {
            candidates.emplace_back(TNodeCandidate{RandomNumber<double>(), nodeDescriptor});
        }
    }
    
    int destinationNodeCount = std::min<int>(candidates.size(), Config_->DestinationNodeCount);
        
    // Pick `destinationNodeCount` uniformly at random.
    std::nth_element(
        candidates.begin(),
        candidates.begin() + destinationNodeCount,
        candidates.end());
    candidates.resize(destinationNodeCount);
    
    std::vector<TNodeDescriptor> nodeDescriptors;
    nodeDescriptors.reserve(destinationNodeCount);
    
    for (const auto& candidate : candidates) {
        nodeDescriptors.emplace_back(candidate.descriptor);
    }
    
    return nodeDescriptors;
}

void TPeerBlockDistributor::UpdateTransmittedBytes()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto interfaceToStatistics = GetNetworkInterfaceStatistics();
    TransmittedBytes_ = 0;
    for (const auto& pair : interfaceToStatistics) {
        const auto& interface = pair.first;
        const auto& statistics = pair.second;
        if (NRe2::TRe2::FullMatch(NRe2::StringPiece(interface), *Config_->NetOutInterfaces)) {
            TransmittedBytes_ += statistics.Tx.Bytes;
        }
    }
}

void TPeerBlockDistributor::OnBlocksDistributed(
    const TString& address,
    const TNodeDescriptor& descriptor,
    const std::vector<TBlockId>& blockIds,
    const TDataNodeServiceProxy::TErrorOrRspPopulateCachePtr& rspOrError)
{
    if (rspOrError.IsOK()) {
        TInstant expirationTime;
        FromProto(&expirationTime, rspOrError.Value()->expiration_time());
        LOG_DEBUG("Populate cache request succeeded, registering node as a peer for populated blocks "
            "(Address: %v, ExpirationTime: %v)",
            address,
            expirationTime);
        TPeerInfo peerInfo(descriptor, expirationTime);
        for (const auto& blockId : blockIds) {
            Bootstrap_->GetControlInvoker()->Invoke(
                BIND(&TPeerBlockTable::UpdatePeer, Bootstrap_->GetPeerBlockTable(), blockId, std::move(peerInfo)));
        }
    } else {
        LOG_DEBUG(rspOrError, "Populate cache request failed (Address: %v)",
            address);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
