#include "p2p_block_distributor.h"

#include "bootstrap.h"
#include "block_peer_table.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/chunk_block_manager.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/master_connector.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <util/random/random.h>

namespace NYT::NDataNode {

using namespace NClusterNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NBus;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = P2PLogger;

////////////////////////////////////////////////////////////////////////////////

TP2PBlockDistributor::TP2PBlockDistributor(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(Bootstrap_->GetConfig()->DataNode->P2PBlockDistributor)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetStorageHeavyInvoker(),
        BIND(&TP2PBlockDistributor::DoIteration, MakeWeak(this)),
        Config_->IterationPeriod))
{
    auto profiler = P2PProfiler;

    profiler.AddFuncCounter("/distributed_bytes", MakeStrong(this), [this] {
        return DistributedBytes_.load();
    });

    OutTrafficGauge_ = profiler.Gauge("/out_traffic");
    OutTrafficThrottlerQueueSizeGauge_ = profiler.Gauge("/out_throttler_queue_size");
    DefaultNetworkPendingOutBytesGauge_ = profiler.Gauge("/default_network_pending_out_bytes");
    TotalOutQueueSizeGauge_ = profiler.Gauge("/total_out_queue_size");
    TotalRequestedBlockSizeGauge_ = profiler.Gauge("/total_requested_block_size");
}

void TP2PBlockDistributor::OnBlockRequested(TBlockId blockId, i64 blockSize)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TotalRequestedBlockSize_ += blockSize;
    RecentlyRequestedBlocks_.Enqueue(blockId);
}

void TP2PBlockDistributor::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    UpdateTransmittedBytes();
    PeriodicExecutor_->Start();
}

void TP2PBlockDistributor::DoIteration()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ProcessNewRequests();
    SweepObsoleteRequests();
    if (ShouldDistributeBlocks()) {
        DistributeBlocks();
    }
}

void TP2PBlockDistributor::SweepObsoleteRequests()
{
    auto now = TInstant::Now();
    while (!RequestHistory_.empty()) {
        TBlockId blockId;
        TInstant requestTime;
        std::tie(requestTime, blockId) = RequestHistory_.front();
        if (requestTime + Config_->WindowLength > now) {
            break;
        }

        auto chunkIt = DistributionHistory_.find(blockId.ChunkId);
        YT_VERIFY(chunkIt != DistributionHistory_.end());

        auto it = chunkIt->second.Blocks.find(blockId.BlockIndex);
        YT_VERIFY(it != chunkIt->second.Blocks.end());

        if (--it->second.RequestCount == 0) {
            chunkIt->second.Blocks.erase(it);
        }
        if (chunkIt->second.Blocks.empty()) {
            DistributionHistory_.erase(chunkIt);
        }

        RequestHistory_.pop();
    }
}

void TP2PBlockDistributor::ProcessNewRequests()
{
    auto now = TInstant::Now();

    RecentlyRequestedBlocks_.DequeueAll(true /* reversed */, [&] (const TBlockId& blockId) {
        RequestHistory_.push(std::make_pair(now, blockId));
        ++DistributionHistory_[blockId.ChunkId].Blocks[blockId.BlockIndex].RequestCount;
    });
}

bool TP2PBlockDistributor::ShouldDistributeBlocks()
{
    i64 oldTransmittedBytes = TransmittedBytes_;
    UpdateTransmittedBytes();
    i64 outTraffic = TransmittedBytes_ - oldTransmittedBytes;

    i64 outThrottlerQueueSize = Bootstrap_->GetOutThrottler(TWorkloadDescriptor())->GetQueueTotalCount();
    i64 defaultNetworkPendingOutBytes = 0;
    if (auto defaultNetwork = Bootstrap_->GetDefaultNetworkName()) {
        defaultNetworkPendingOutBytes = TTcpDispatcher::Get()->GetCounters(*defaultNetwork)->PendingOutBytes;
    }
    i64 totalOutQueueSize = outThrottlerQueueSize + defaultNetworkPendingOutBytes;

    i64 totalRequestedBlockSize = TotalRequestedBlockSize_;

    bool shouldDistributeBlocks =
        outTraffic > static_cast<double>(Config_->OutTrafficActivationThreshold) * Config_->IterationPeriod.MilliSeconds() / 1000.0 ||
        totalOutQueueSize > Config_->OutQueueSizeActivationThreshold ||
        totalRequestedBlockSize > Config_->TotalRequestedBlockSizeActivationThreshold * Config_->IterationPeriod.MilliSeconds()  / 1000.0;

    YT_LOG_DEBUG("Determining if blocks should be distributed (IterationPeriod: %v, OutTraffic: %v, "
        "OutTrafficActivationThreshold: %v, OutThrottlerQueueSize: %v, DefaultNetworkPendingOutBytes: %v, "
        "TotalOutQueueSize: %v, OutQueueSizeActivationThreshold: %v, TotalRequestedBlockSize: %v, "
        "TotalRequestedBlockSizeActivationThreshold: %v, ShouldDistributeBlocks: %v)",
        Config_->IterationPeriod,
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

    // Profile all related values.
    OutTrafficGauge_.Update(outTraffic);
    OutTrafficThrottlerQueueSizeGauge_.Update(outThrottlerQueueSize);
    DefaultNetworkPendingOutBytesGauge_.Update(defaultNetworkPendingOutBytes);
    TotalOutQueueSizeGauge_.Update(totalOutQueueSize);
    TotalRequestedBlockSizeGauge_.Update(totalRequestedBlockSize);

    return shouldDistributeBlocks;
}

void TP2PBlockDistributor::DistributeBlocks()
{
    auto chosenBlocks = ChooseBlocks();
    const auto& reqTemplates = chosenBlocks.ReqTemplates;
    const auto& blocks = chosenBlocks.Blocks;
    const auto& blockIds = chosenBlocks.BlockIds;
    auto totalBlockSize = chosenBlocks.TotalSize;

    if (blocks.empty()) {
        YT_LOG_DEBUG("No blocks may be distributed on current iteration");
        return;
    }

    YT_LOG_INFO("Ready to distribute blocks (BlockCount: %v, TotalBlockSize: %v)",
        blocks.size(),
        totalBlockSize);

    auto now = TInstant::Now();
    for (const auto& blockId : blockIds) {
        DistributionHistory_[blockId.ChunkId].Blocks[blockId.BlockIndex].LastDistributionTime = now;
    }

    YT_VERIFY(blocks.size() == blockIds.size() && blocks.size() == reqTemplates.size());

    const auto& channelFactory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetChannelFactory();

    const auto& blockPeerTable = Bootstrap_->GetBlockPeerTable();

    // Filter nodes that are not local and that are allowed by node tag filter.
    auto nodes = Bootstrap_->GetNodeDirectory()->GetAllDescriptors();
    auto localNodeId = Bootstrap_->GetNodeId();
    nodes.erase(std::remove_if(nodes.begin(), nodes.end(), [&] (const auto& pair) {
        return
            pair.first == localNodeId ||
            !Config_->NodeTagFilter.IsSatisfiedBy(pair.second.GetTags());
    }), nodes.end());
    THashMap<TNodeId, TNodeDescriptor> nodeSet{nodes.begin(), nodes.end()};

    for (size_t index = 0; index < blocks.size(); ++index) {
        const auto& block = blocks[index];
        const auto& blockId = blockIds[index];
        const auto& reqTemplate = reqTemplates[index];
        auto& chunk = DistributionHistory_[blockId.ChunkId];

        auto peerList = blockPeerTable->GetOrCreatePeerList(blockId);

        auto destinationNodes = ChooseDestinationNodes(nodeSet, nodes, peerList, &chunk.Nodes);
        if (destinationNodes.empty()) {
            YT_LOG_WARNING("No suitable destination nodes found");
            // We have no chances to succeed with following blocks.
            break;
        }

        YT_LOG_DEBUG("Sending block to destination nodes (BlockId: %v, DestinationNodes: %v)",
            blockId,
            MakeFormattableView(destinationNodes, [] (auto* builder, const auto& pair) {
                 FormatValue(builder, *pair.second, TStringBuf());
             }));

        for (const auto& destinationNode : destinationNodes) {
            const auto& [nodeId, nodeDescriptor] = destinationNode;
            const auto& destinationAddress = nodeDescriptor->GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
            auto heavyChannel = CreateRetryingChannel(
                Config_->NodeChannel,
                channelFactory->CreateChannel(destinationAddress));
            TDataNodeServiceProxy proxy(std::move(heavyChannel));
            auto req = proxy.PopulateCache();
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            req->MergeFrom(reqTemplate);
            SetRpcAttachedBlocks(req, {block});
            req->Invoke().Subscribe(BIND(
                &TP2PBlockDistributor::OnBlockDistributed,
                MakeWeak(this),
                destinationAddress,
                nodeId,
                blockId,
                block.Size()));
        }
    }
}

TP2PBlockDistributor::TChosenBlocks TP2PBlockDistributor::ChooseBlocks()
{
    // First we filter the blocks requested during the considered window (`Config->WindowLength` from now) such that:
    // 1) Block was not recently distributed (within `Config_->ConsecutiveDistributionDelay` from now);
    // 2) Block does not have many peers (at most `Config_->MaxBlockPeerCount`);
    // 3) Block has been requested at least `Config_->MinRequestCount`.
    // These candidate blocks are sorted in a descending order of request count.
    // We iterate over the blocks forming a PopulateCache request of total size no more than
    // `Config_->MaxPopulateRequestSize`, and finally deliver each of them to no more than
    // `Config_->DestinationNodeCountPerIteration` nodes, marking them as peers to the processed blocks.

    auto now = TInstant::Now();

    struct TBlockCandidate
    {
        TBlockId BlockId;
        TInstant LastDistributionTime;
        int DistributionCount;
        int RequestCount;

        bool operator <(const TBlockCandidate& other) const
        {
            if (RequestCount != other.RequestCount) {
                return RequestCount > other.RequestCount;
            }
            if (DistributionCount != other.DistributionCount) {
                return DistributionCount < other.DistributionCount;
            }
            return false;
        }
    };

    std::vector<TBlockCandidate> candidates;
    for (const auto& [chunkId, chunkHistory] : DistributionHistory_) {
        for (const auto& [blockIndex, distributionEntry] : chunkHistory.Blocks) {
            TBlockId blockId{chunkId, blockIndex};

            YT_VERIFY(distributionEntry.RequestCount > 0);
            if (distributionEntry.LastDistributionTime + Config_->ConsecutiveDistributionDelay <= now &&
                distributionEntry.DistributionCount <= Config_->MaxDistributionCount &&
                distributionEntry.RequestCount >= Config_->MinRequestCount)
            {
                candidates.emplace_back(TBlockCandidate{
                    blockId,
                    distributionEntry.LastDistributionTime,
                    distributionEntry.DistributionCount,
                    distributionEntry.RequestCount});
            }
        }
    }

    std::sort(candidates.begin(), candidates.end());

    TP2PBlockDistributor::TChosenBlocks chosenBlocks;

    const auto& blockCache = Bootstrap_->GetBlockCache();
    const auto& distributionThrottler = Bootstrap_->GetThrottler(EDataNodeThrottlerKind::P2POut);

    for (const auto& candidate : candidates) {
        if (distributionThrottler->IsOverdraft()) {
            break;
        }

        auto blockId = candidate.BlockId;
        auto cachedBlock = blockCache->FindBlock(blockId, EBlockType::CompressedData);
        if (!cachedBlock.Block) {
            // TODO(max42): the block is both hot enough to be distributed,
            // but missing in the block cache? Sounds strange, but maybe we
            // should fetch it from the disk then?
            YT_LOG_DEBUG("Candidate block is missing in chunk block manager cache (BlockId: %v, RequestCount: %v, "
                "LastDistributionTime: %v, DistributionCount: %v)",
                blockId,
                candidate.RequestCount,
                candidate.LastDistributionTime,
                candidate.DistributionCount);
            continue;
        }

        int requestCount = candidate.RequestCount;
        auto lastDistributionTime = candidate.LastDistributionTime;
        int distributionCount = candidate.DistributionCount;
        i64 blockSize = cachedBlock.Block.Size();
        auto block = cachedBlock.Block;
        if (chosenBlocks.TotalSize + blockSize <= Config_->MaxPopulateRequestSize || chosenBlocks.TotalSize == 0) {
            YT_LOG_DEBUG("Block is ready for distribution (BlockId: %v, RequestCount: %v, LastDistributionTime: %v, "
                "DistributionCount: %v, Size: %v)",
                blockId,
                requestCount,
                lastDistributionTime,
                distributionCount,
                blockSize);
            chosenBlocks.ReqTemplates.emplace_back();
            auto& reqTemplate = chosenBlocks.ReqTemplates.back();
            auto* protoBlock = reqTemplate.add_blocks();
            ToProto(protoBlock->mutable_block_id(), blockId);
            chosenBlocks.Blocks.emplace_back(std::move(block));
            chosenBlocks.BlockIds.emplace_back(blockId);
            chosenBlocks.TotalSize += blockSize;

            distributionThrottler->Acquire(blockSize);
        }
    }

    return chosenBlocks;
}

std::vector<std::pair<TNodeId, const TNodeDescriptor*>> TP2PBlockDistributor::ChooseDestinationNodes(
    const THashMap<TNodeId, TNodeDescriptor>& nodeSet,
    const std::vector<std::pair<TNodeId, TNodeDescriptor>>& nodes,
    const TCachedPeerListPtr& peerList,
    THashSet<TNodeId>* preferredPeers) const
{
    auto activePeerList = peerList->GetPeers();
    THashSet<TNodeId> activePeers{activePeerList.begin(), activePeerList.end()};

    THashMap<TNodeId, const TNodeDescriptor*> destinationNodes;

    auto addPeer = [&] (auto nodeId) {
        if (activePeers.find(nodeId) != activePeers.end()) {
            return false;
        }

        auto it = nodeSet.find(nodeId);
        if (it == nodeSet.end()) {
            return false;
        }

        destinationNodes[nodeId] = &(it->second);
        return std::ssize(destinationNodes) >= Config_->DestinationNodeCount;
    };

    bool done = false;
    for (auto peer : *preferredPeers) {
        if (addPeer(peer)) {
            done = true;
            break;
        }
    }

    if (!done) {
        while (destinationNodes.size() + activePeers.size() < nodes.size()) {
            auto index = RandomNumber<size_t>(nodes.size());
            auto randomPeer = nodes[index].first;

            preferredPeers->insert(randomPeer);
            if (addPeer(randomPeer)) {
                break;
            }
        }
    }

    return {destinationNodes.begin(), destinationNodes.end()};
}

void TP2PBlockDistributor::UpdateTransmittedBytes()
{
    TNetworkInterfaceStatisticsMap interfaceToStatistics;
    try {
        interfaceToStatistics = GetNetworkInterfaceStatistics();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Error getting network interface statistics");
        return;
    }

    TransmittedBytes_ = 0;
    for (const auto& [interface, statistics] : interfaceToStatistics) {
        if (NRe2::TRe2::FullMatch(NRe2::StringPiece(interface), *Config_->NetOutInterfaces)) {
            TransmittedBytes_ += statistics.Tx.Bytes;
        }
    }
}

void TP2PBlockDistributor::OnBlockDistributed(
    const TString& address,
    const TNodeId nodeId,
    const TBlockId& blockId,
    i64 size,
    const TDataNodeServiceProxy::TErrorOrRspPopulateCachePtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!rspOrError.IsOK()) {
        YT_LOG_DEBUG(rspOrError, "Populate cache request failed (Address: %v)",
            address);
        return;
    }

    const auto& rsp = rspOrError.Value();
    auto expirationDeadline = FromProto<TInstant>(rsp->expiration_deadline());

    YT_LOG_DEBUG("Populate cache request succeeded, registering node as a peer for block "
        "(BlockId: %v, Address: %v, NodeId: %v, ExpirationDeadline: %v, Size: %v)",
        blockId,
        address,
        nodeId,
        expirationDeadline,
        size);

    const auto& blockPeerTable = Bootstrap_->GetBlockPeerTable();
    auto peerList = blockPeerTable->GetOrCreatePeerList(blockId);
    peerList->AddPeer(nodeId, expirationDeadline);

    DistributedBytes_ += size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
