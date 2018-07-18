#include "peer_block_distributor.h"

#include "peer_block_table.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/config.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/misc/proc.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/retrying_channel.h>
#include <yt/core/rpc/dispatcher.h>

#include <yt/core/bus/tcp/dispatcher.h>

#include <util/random/random.h>

namespace NYT {
namespace NDataNode {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NBus;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = P2PLogger;
static const auto& Profiler = P2PProfiler;

////////////////////////////////////////////////////////////////////////////////

TPeerBlockDistributor::TPeerBlockDistributor(TPeerBlockDistributorConfigPtr config, TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , Invoker_(CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TPeerBlockDistributor::DoIteration, MakeWeak(this)),
        Config_->IterationPeriod))
{ }

void TPeerBlockDistributor::OnBlockRequested(TBlockId blockId, i64 blockSize)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TotalRequestedBlockSize_ += blockSize;
    RecentlyRequestedBlocks_.Enqueue(blockId);
}

void TPeerBlockDistributor::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    UpdateTransmittedBytes();
    PeriodicExecutor_->Start();
}

void TPeerBlockDistributor::DoIteration()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    ProcessNewRequests();
    SweepObsoleteRequests();

    if (ShouldDistributeBlocks()) {
        DistributeBlocks();
    }
    Profiler.Enqueue("/distributed_block_size", DistributedBytes_, EMetricType::Counter);
}

void TPeerBlockDistributor::SweepObsoleteRequests()
{
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
    auto now = TInstant::Now();

    RecentlyRequestedBlocks_.DequeueAll(true /* reversed */, [&] (const TBlockId& blockId) {
        RequestHistory_.push(std::make_pair(now, blockId));
        ++BlockIdToDistributionEntry_[blockId].RequestCount;
    });
}

bool TPeerBlockDistributor::ShouldDistributeBlocks()
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
        outTraffic >= Config_->OutTrafficActivationThreshold * Config_->IterationPeriod.Seconds() ||
        totalOutQueueSize >= Config_->OutQueueSizeActivationThreshold ||
        totalRequestedBlockSize >= Config_->TotalRequestedBlockSizeActivationThreshold * Config_->IterationPeriod.Seconds();

    LOG_DEBUG("Determining if blocks should be distributed (IterationPeriod: %v, OutTraffic: %v, "
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
    Profiler.Enqueue("/out_traffic", outTraffic, EMetricType::Gauge);
    Profiler.Enqueue("/out_throttler_queue_size", outThrottlerQueueSize, EMetricType::Gauge);
    Profiler.Enqueue("/default_network_pending_out_bytes", defaultNetworkPendingOutBytes, EMetricType::Gauge);
    Profiler.Enqueue("/total_out_queue_size", totalOutQueueSize, EMetricType::Gauge);
    Profiler.Enqueue("/total_requested_block_size", totalRequestedBlockSize, EMetricType::Gauge);

    return shouldDistributeBlocks;
}

void TPeerBlockDistributor::DistributeBlocks()
{
    auto chosenBlocks = ChooseBlocks();
    const auto& reqTemplates = chosenBlocks.ReqTemplates;
    const auto& blocks = chosenBlocks.Blocks;
    const auto& blockIds = chosenBlocks.BlockIds;
    auto totalBlockSize = chosenBlocks.TotalSize;

    if (blocks.empty()) {
        LOG_DEBUG("No blocks may be distributed on current iteration");
        return;
    }

    LOG_INFO("Ready to distribute blocks (BlockCount: %v, TotalBlockSize: %v)",
        blocks.size(),
        totalBlockSize);

    auto now = TInstant::Now();
    for (const auto& blockId : blockIds) {
        BlockIdToDistributionEntry_[blockId].LastDistributionTime = now;
    }

    YCHECK(blocks.size() == blockIds.size() && blocks.size() == reqTemplates.size());

    const auto& channelFactory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetChannelFactory();

    // Filter nodes that are not local and that are allowed by node tag filter.
    auto nodes = Bootstrap_->GetNodeDirectory()->GetAllDescriptors();
    auto localDescriptor = Bootstrap_->GetMasterConnector()->GetLocalDescriptor();
    nodes.erase(std::remove_if(nodes.begin(), nodes.end(), [&] (const auto& nodeDescriptor) {
        return
            nodeDescriptor.GetDefaultAddress() == localDescriptor.GetDefaultAddress() ||
            !Config_->NodeTagFilter.IsSatisfiedBy(nodeDescriptor.GetTags());
    }), nodes.end());

    for (size_t index = 0; index < blocks.size(); ++index) {
        // TODO(max42): maybe we should try to avoid the nodes already having our block here
        // using the information from peer block table.
        auto destinationNodes = ChooseDestinationNodes(nodes);
        if (destinationNodes.empty()) {
            LOG_WARNING("No suitable destination nodes found");
            // We have no chances to succeed with following blocks.
            break;
        }

        const auto& block = blocks[index];
        const auto& blockId = blockIds[index];
        const auto& reqTemplate = reqTemplates[index];;

        LOG_DEBUG("Sending block to destination nodes (BlockId: %v, DestinationNodes: %v)",
            blockId,
            destinationNodes);

        for (const auto& destinationNode : destinationNodes) {
            const auto& destinationAddress = destinationNode.GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
            auto heavyChannel = CreateRetryingChannel(
                Config_->NodeChannel,
                channelFactory->CreateChannel(destinationAddress));
            TDataNodeServiceProxy proxy(std::move(heavyChannel));
            auto req = proxy.PopulateCache();
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            req->MergeFrom(reqTemplate);
            SetRpcAttachedBlocks(req, {block});
            req->Invoke().Subscribe(BIND(
                &TPeerBlockDistributor::OnBlockDistributed,
                MakeWeak(this),
                destinationAddress,
                destinationNode,
                blockId,
                block.Size())
                .Via(Bootstrap_->GetControlInvoker()));
        }
    }
}

TPeerBlockDistributor::TChosenBlocks TPeerBlockDistributor::ChooseBlocks()
{
    // First we filter the blocks requested during the considered window (`Config->WindowLength` from now) such that:
    // 1) Block was not recently distributed (within `Config_->ConsecutiveDistributionDelay` from now);
    // 2) Block does not have many peers (at most `Config_->MaxBlockPeerCount`);
    // 3) Block has been requested at least `Config_->MinRequestConit`.
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
    for (const auto& pair : BlockIdToDistributionEntry_) {
        const auto& blockId = pair.first;
        const auto& distributionEntry = pair.second;
        YCHECK(distributionEntry.RequestCount > 0);
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

    std::sort(candidates.begin(), candidates.end());

    TPeerBlockDistributor::TChosenBlocks chosenBlocks;

    const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();

    for (const auto& candidate : candidates) {
        auto blockId = candidate.BlockId;
        auto cachedBlock = chunkBlockManager->FindCachedBlock(blockId);
        if (!cachedBlock) {
            // TODO(max42): the block is both hot enough to be distributed,
            // but missing in the block cache? Sounds strange, but maybe we
            // should fetch it from the disk then?
            LOG_DEBUG("Candidate block is missing in chunk block manager cache (BlockId: %v, RequestCount: %v, "
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
        i64 blockSize = cachedBlock->GetData().Size();
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
        if (chosenBlocks.TotalSize + blockSize <= Config_->MaxPopulateRequestSize || chosenBlocks.TotalSize == 0) {
            LOG_DEBUG("Block is ready for distribution (BlockId: %v, RequestCount: %v, LastDistributionTime: %v, "
                "DistributionCount: %v, Source: %v, Size: %v)",
                blockId,
                requestCount,
                lastDistributionTime,
                distributionCount,
                source,
                blockSize);
            chosenBlocks.ReqTemplates.emplace_back();
            auto& reqTemplate = chosenBlocks.ReqTemplates.back();
            auto* protoBlock = reqTemplate.add_blocks();
            ToProto(protoBlock->mutable_block_id(), blockId);
            if (source) {
                ToProto(protoBlock->mutable_source_descriptor(), *source);
            }
            chosenBlocks.Blocks.emplace_back(std::move(block));
            chosenBlocks.BlockIds.emplace_back(blockId);
            chosenBlocks.TotalSize += blockSize;
        }
    }

    return chosenBlocks;
}

std::vector<TNodeDescriptor> TPeerBlockDistributor::ChooseDestinationNodes(const std::vector<TNodeDescriptor>& nodes) const
{
    THashSet<TNodeDescriptor> destinationNodes;

    while (destinationNodes.size() < Config_->DestinationNodeCount && destinationNodes.size() < nodes.size()) {
        auto index = RandomNumber<size_t>(nodes.size());
        destinationNodes.insert(nodes[index]);
    }

    return std::vector<TNodeDescriptor>(destinationNodes.begin(), destinationNodes.end());
}

void TPeerBlockDistributor::UpdateTransmittedBytes()
{
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

void TPeerBlockDistributor::OnBlockDistributed(
    const TString& address,
    const TNodeDescriptor& descriptor,
    const TBlockId& blockId,
    i64 size,
    const TDataNodeServiceProxy::TErrorOrRspPopulateCachePtr& rspOrError)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    if (!rspOrError.IsOK()) {
        LOG_DEBUG(rspOrError, "Populate cache request failed (Address: %v)",
            address);
        return;
    }

    TInstant expirationTime;
    FromProto(&expirationTime, rspOrError.Value()->expiration_time());
    LOG_DEBUG("Populate cache request succeeded, registering node as a peer for block "
        "(BlockId: %v, Address: %v, ExpirationTime: %v, Size: %v)",
        blockId,
        address,
        expirationTime,
        size);
    DistributedBytes_ += size;
    TPeerInfo peerInfo(descriptor, expirationTime);
    Bootstrap_->GetPeerBlockTable()->UpdatePeer(blockId, std::move(peerInfo));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
