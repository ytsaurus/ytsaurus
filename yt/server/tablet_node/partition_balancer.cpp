#include "partition_balancer.h"
#include "private.h"
#include "sorted_chunk_store.h"
#include "config.h"
#include "partition.h"
#include "slot_manager.h"
#include "store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/mutation.h>

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/samples_fetcher.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NTableClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NTabletNode::NProto;

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancer
    : public TRefCounted
{
public:
    TPartitionBalancer(
        TPartitionBalancerConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Semaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentSamplings))
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TPartitionBalancer::OnScanSlot, MakeStrong(this)));
    }

private:
    TPartitionBalancerConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;
    TAsyncSemaphorePtr Semaphore_;


    void OnScanSlot(TTabletSlotPtr slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        auto tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(TTabletSlotPtr slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (!tablet->IsSorted()) {
            return;
        }
        
        for (const auto& partition : tablet->PartitionList()) {
            ScanPartition(slot, partition.get());
        }
    }

    void ScanPartition(TTabletSlotPtr slot, TPartition* partition)
    {
        auto* tablet = partition->GetTablet();

        const auto& config = tablet->GetConfig();

        int partitionCount = tablet->PartitionList().size();

        i64 actualDataSize = partition->GetUncompressedDataSize();

        // Maximum data size the partition might have if all chunk stores from Eden go here.
        i64 maxPotentialDataSize = actualDataSize;
        for (const auto& store : tablet->GetEden()->Stores()) {
            if (store->GetType() == EStoreType::SortedChunk) {
                maxPotentialDataSize += store->GetUncompressedDataSize();
            }
        }

        if (actualDataSize > config->MaxPartitionDataSize) {
            int splitFactor = std::min(std::min(
                actualDataSize / config->DesiredPartitionDataSize + 1,
                actualDataSize / config->MinPartitioningDataSize),
                static_cast<i64>(config->MaxPartitionCount - partitionCount));
            if (splitFactor > 1) {
                RunSplit(slot, partition, splitFactor);
            }
        }
        
        if (maxPotentialDataSize < config->MinPartitionDataSize && partitionCount > 1) {
            int firstPartitionIndex = partition->GetIndex();
            int lastPartitionIndex = firstPartitionIndex + 1;
            if (lastPartitionIndex == partitionCount) {
                --firstPartitionIndex;
                --lastPartitionIndex;
            }
            RunMerge(slot, partition, firstPartitionIndex, lastPartitionIndex);
        }

        if (partition->GetSamplingRequestTime() > partition->GetSamplingTime() &&
            partition->GetSamplingTime() < TInstant::Now() - Config_->ResamplingPeriod)
        {
            RunSample(slot, partition);
        }
    }


    void RunSplit(TTabletSlotPtr slot, TPartition* partition, int splitFactor)
    {
        if (partition->GetState() != EPartitionState::Normal) {
            return;
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetStoreState() != EStoreState::Persistent) {
                return;
            }
        }

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Splitting);

        BIND(&TPartitionBalancer::DoRunSplit, MakeStrong(this))
            .AsyncVia(partition->GetTablet()->GetEpochAutomatonInvoker())
            .Run(slot, partition, splitFactor);
    }

    void DoRunSplit(TTabletSlotPtr slot, TPartition* partition, int splitFactor)
    {
        auto Logger = BuildLogger(partition);

        auto* tablet = partition->GetTablet();
        auto hydraManager = slot->GetHydraManager();

        LOG_INFO("Partition is eligible for split (SplitFactor: %v)",
            splitFactor);

        try {
            auto samples = GetPartitionSamples(partition, Config_->MaxPartitioningSampleCount);
            int sampleCount = static_cast<int>(samples.size());
            int minSampleCount = std::max(Config_->MinPartitioningSampleCount, splitFactor);
            if (sampleCount < minSampleCount) {
                THROW_ERROR_EXCEPTION("Too few samples fetched: need %v, got %v",
                    minSampleCount,
                    sampleCount);
            }

            std::vector<TOwningKey> pivotKeys;
            // Take the pivot of the partition.
            pivotKeys.push_back(partition->GetPivotKey());
            // And add |splitFactor - 1| more keys from samples.
            for (int i = 0; i < splitFactor - 1; ++i) {
                int j = (i + 1) * sampleCount / splitFactor - 1;
                const auto& key = samples[j];
                if (key > pivotKeys.back()) {
                    pivotKeys.push_back(key);
                }
            }

            if (pivotKeys.size() < 2) {
                THROW_ERROR_EXCEPTION("No valid pivot keys can be obtained from samples");
            }

            TReqSplitPartition request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            ToProto(request.mutable_partition_id(), partition->GetId());
            ToProto(request.mutable_pivot_keys(), pivotKeys);

            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Partitioning aborted");
            partition->CheckedSetState(EPartitionState::Splitting, EPartitionState::Normal);
        }
    }


    void RunMerge(
        TTabletSlotPtr slot,
        TPartition* partition,
        int firstPartitionIndex,
        int lastPartitionIndex)
    {
        auto* tablet = partition->GetTablet();

        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            if (tablet->PartitionList()[index]->GetState() != EPartitionState::Normal) {
                return;
            }
        }

        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            tablet->PartitionList()[index]->CheckedSetState(EPartitionState::Normal, EPartitionState::Merging);
        }

        auto Logger = TabletNodeLogger;
        Logger.AddTag("TabletId: %v, PartitionIds: %v",
            partition->GetTablet()->GetId(),
            MakeFormattableRange(
                MakeRange(
                    tablet->PartitionList().data() + firstPartitionIndex,
                    tablet->PartitionList().data() + lastPartitionIndex + 1),
                TPartitionIdFormatter()));

        LOG_INFO("Partition is eligible for merge");

        auto hydraManager = slot->GetHydraManager();

        TReqMergePartitions request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        ToProto(request.mutable_partition_id(), tablet->PartitionList()[firstPartitionIndex]->GetId());
        request.set_partition_count(lastPartitionIndex - firstPartitionIndex + 1);

        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }


    void RunSample(TTabletSlotPtr slot, TPartition* partition)
    {
        if (partition->GetState() != EPartitionState::Normal) {
            return;
        }

        auto guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_);
        if (!guard) {
            return;
        }

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Sampling);

        BIND(&TPartitionBalancer::DoRunSample, MakeStrong(this), Passed(std::move(guard)))
            .AsyncVia(partition->GetTablet()->GetEpochAutomatonInvoker())
            .Run(slot, partition);
    }

    void DoRunSample(
        TAsyncSemaphoreGuard /*guard*/,
        TTabletSlotPtr slot,
        TPartition* partition)
    {
        auto Logger = BuildLogger(partition);

        auto* tablet = partition->GetTablet();
        auto config = tablet->GetConfig();

        auto hydraManager = slot->GetHydraManager();

        LOG_INFO("Sampling partition (DesiredSampleCount: %v)",
            config->SamplesPerPartition);

        try {
            auto samples = GetPartitionSamples(partition, config->SamplesPerPartition);
            samples.erase(
                std::unique(samples.begin(), samples.end()),
                samples.end());

            TReqUpdatePartitionSampleKeys request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            ToProto(request.mutable_partition_id(), partition->GetId());
            ToProto(request.mutable_sample_keys(), samples);

            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Partition sampling aborted");
        }

        partition->CheckedSetState(EPartitionState::Sampling, EPartitionState::Normal);
        // NB: Update the timestamp even in case of failure to prevent
        // repeating unsuccessful samplings too rapidly.
        partition->SetSamplingTime(TInstant::Now());
    }


    std::vector<TOwningKey> GetPartitionSamples(
        TPartition* partition,
        int maxSampleCount)
    {
        YCHECK(!partition->IsEden());

        if (maxSampleCount == 0) {
            return std::vector<TOwningKey>();
        }

        auto Logger = BuildLogger(partition);

        auto* tablet = partition->GetTablet();

        auto nodeDirectory = New<TNodeDirectory>();

        auto fetcher = New<TSamplesFetcher>(
            Config_->SamplesFetcher,
            maxSampleCount,
            tablet->Schema().GetKeyColumns(),
            std::numeric_limits<i64>::max(),
            nodeDirectory,
            GetCurrentInvoker(),
            TScrapeChunksCallback(),
            Bootstrap_->GetMasterClient(),
            Logger);

        {
            // XXX(babenko): multicell
            auto channel = Bootstrap_->GetMasterClient()->GetMasterChannelOrThrow(
                NApi::EMasterChannelKind::LeaderOrFollower);
            TChunkServiceProxy proxy(channel);

            auto req = proxy.LocateChunks();

            yhash_map<TChunkId, TSortedChunkStorePtr> storeMap;

            auto addStore = [&] (const ISortedStorePtr& store) {
                if (store->GetType() != EStoreType::SortedChunk)
                    return;

                if (store->GetMaxKey() <= partition->GetPivotKey() ||
                    store->GetMinKey() >= partition->GetNextPivotKey())
                    return;

                const auto& chunkId = store->GetId();
                YCHECK(storeMap.insert(std::make_pair(chunkId, store->AsSortedChunk())).second);
                ToProto(req->add_subrequests(), chunkId);
            };

            auto addStores = [&] (const yhash_set<ISortedStorePtr>& stores) {
                for (const auto& store : stores) {
                    addStore(store);
                }
            };

            addStores(partition->Stores());
            addStores(tablet->GetEden()->Stores());

            if (req->subrequests_size() == 0) {
                return std::vector<TOwningKey>();
            }

            LOG_INFO("Locating partition chunks (ChunkCount: %v)",
                req->subrequests_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error locating partition chunks");
            const auto& rsp = rspOrError.Value();
            YCHECK(req->subrequests_size() == rsp->subresponses_size());

            LOG_INFO("Partition chunks located");

            nodeDirectory->MergeFrom(rsp->node_directory());

            for (int index = 0; index < rsp->subresponses_size(); ++index) {
                const auto& subrequest = req->subrequests(index);
                const auto& subresponse = rsp->subresponses(index);
                auto chunkId = FromProto<TChunkId>(subrequest);
                auto storeIt = storeMap.find(chunkId);
                YCHECK(storeIt != storeMap.end());
                auto store = storeIt->second;
                auto chunkSpec = New<TRefCountedChunkSpec>();
                ToProto(chunkSpec->mutable_chunk_id(), chunkId);
                chunkSpec->mutable_replicas()->MergeFrom(subresponse.replicas());
                chunkSpec->mutable_chunk_meta()->CopyFrom(store->GetChunkMeta());
                ToProto(chunkSpec->mutable_lower_limit(), TReadLimit(partition->GetPivotKey()));
                ToProto(chunkSpec->mutable_upper_limit(), TReadLimit(partition->GetNextPivotKey()));
                chunkSpec->set_erasure_codec(subresponse.erasure_codec());
                fetcher->AddChunk(chunkSpec);
            }
        }

        WaitFor(fetcher->Fetch())
            .ThrowOnError();

        std::vector<TOwningKey> samples;
        for (const auto& sample : fetcher->GetSamples()) {
            YCHECK(!sample.Incomplete);
            YCHECK(sample.Weight == 1);
            samples.push_back(sample.Key);
        }

        // NB(psushin): This filtering is typically redundant (except for the first pivot), 
        // since fetcher already returns samples within given limits.
        samples.erase(
            std::remove_if(
                samples.begin(),
                samples.end(),
                [&] (const TOwningKey& key) {
                    return key <= partition->GetPivotKey() || key >= partition->GetNextPivotKey();
                }),
            samples.end());

        std::sort(samples.begin(), samples.end());
        return samples;
    }


    static NLogging::TLogger BuildLogger(TPartition* partition)
    {
        auto logger = TabletNodeLogger;
        logger.AddTag("TabletId: %v, PartitionId: %v",
            partition->GetTablet()->GetId(),
            partition->GetId());
        return logger;
    }
};

void StartPartitionBalancer(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    if (config->EnablePartitionBalancer) {
        New<TPartitionBalancer>(config->PartitionBalancer, bootstrap);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
