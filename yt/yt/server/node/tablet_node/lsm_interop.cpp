#include "lsm_interop.h"

#include "bootstrap.h"
#include "partition_balancer.h"
#include "private.h"
#include "slot_manager.h"
#include "slot_manager.h"
#include "store.h"
#include "sorted_chunk_store.h"
#include "store_compactor.h"
#include "store_manager.h"
#include "store_rotator.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>

#include <yt/yt/server/lib/lsm/partition.h>
#include <yt/yt/server/lib/lsm/store.h>
#include <yt/yt/server/lib/lsm/lsm_backend.h>
#include <yt/yt/server/lib/lsm/tablet.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TLsmInterop
    : public ILsmInterop
{
public:
    TLsmInterop(
        IBootstrap* bootstrap,
        const IStoreCompactorPtr& storeCompactor,
        const IPartitionBalancerPtr& partitionBalancer,
        const IStoreRotatorPtr& storeRotator)
        : Bootstrap_(bootstrap)
        , StoreCompactor_(storeCompactor)
        , PartitionBalancer_(partitionBalancer)
        , StoreRotator_(storeRotator)
        , Backend_(NLsm::CreateLsmBackend())
        , RowDigestCache_(/*maxWeight*/ 0)
        , RowDigestRequestThrottler_(CreateReconfigurableThroughputThrottler(New<TThroughputThrottlerConfig>()))
    { }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&TLsmInterop::OnBeginSlotScan, MakeWeak(this)));
        slotManager->SubscribeScanSlot(BIND(&TLsmInterop::OnScanSlot, MakeWeak(this)));
        slotManager->SubscribeEndSlotScan(BIND(&TLsmInterop::OnEndSlotScan, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TLsmInterop::OnDynamicConfigChanged, MakeWeak(this)));

        // Do not depend on initialization order of lsm interop and dynamic config manager.
        OnDynamicConfigChanged(nullptr, dynamicConfigManager->GetConfig());

        Profiler.AddFuncGauge("/row_digest_cache_item_count", MakeStrong(this), [this] {
            auto guard = Guard(RowDigestCacheLock_);
            return RowDigestCache_.GetSize();
        });
    }

private:
    IBootstrap* const Bootstrap_;
    const IStoreCompactorPtr StoreCompactor_;
    const IPartitionBalancerPtr PartitionBalancer_;
    const IStoreRotatorPtr StoreRotator_;
    const NLsm::ILsmBackendPtr Backend_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, RowDigestCacheLock_);
    TSimpleLruCache<TStoreId, NTableClient::NProto::TVersionedRowDigestExt> RowDigestCache_;
    IReconfigurableThroughputThrottlerPtr RowDigestRequestThrottler_;
    std::atomic<bool> UseRowDigests_;

    NProfiling::TProfiler Profiler = TabletNodeProfiler.WithPrefix("/lsm_interop");
    const NProfiling::TCounter RowDigestRequestCount_ = Profiler.Counter("/row_digest_request_count");
    const NProfiling::TCounter FailedRowDigestRequestCount_ = Profiler.Counter("/failed_row_digest_request_count");
    const NProfiling::TCounter ThrottledRowDigestRequestCount_ = Profiler.Counter("/throttled_row_digest_request_count");
    const NProfiling::TTimeCounter RowDigestParseCumulativeTime_ = Profiler.TimeCounter("/row_digest_parse_cumulative_time");

    void OnDynamicConfigChanged(
        TClusterNodeDynamicConfigPtr /*oldConfig*/,
        TClusterNodeDynamicConfigPtr newConfig)
    {
        const auto& config = newConfig->TabletNode->StoreCompactor;
        RowDigestRequestThrottler_->Reconfigure(config->RowDigestRequestThrottler);
        UseRowDigests_.store(config->UseRowDigests);

        {
            auto guard = Guard(RowDigestCacheLock_);
            RowDigestCache_.SetMaxWeight(config->RowDigestCacheSize);
        }
    }

    void OnBeginSlotScan()
    {
        YT_LOG_DEBUG("LSM interop begins slot scan");

        StoreCompactor_->OnBeginSlotScan();

        SetBackendState();
    }

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        if (slot->GetAutomatonState() != NHydra::EPeerState::Leading) {
            return;
        }

        YT_LOG_DEBUG("LSM interop scans slot (CellId: %v)", slot->GetCellId());

        const auto& tabletManager = slot->GetTabletManager();

        std::vector<NLsm::TTabletPtr> lsmTablets;

        std::vector<TFuture<void>> asyncRequests;

        {
            TForbidContextSwitchGuard guard;

            for (auto [tabletId, tablet] : tabletManager->Tablets()) {
                lsmTablets.push_back(ScanTablet(slot, tablet));
            }
        }

        YT_LOG_DEBUG("Tablets collected (CellId: %v, TabletCount: %v)",
            slot->GetCellId(),
            lsmTablets.size());

        auto actions = Backend_->BuildLsmActions(lsmTablets, slot->GetTabletCellBundleName());
        StoreCompactor_->ProcessLsmActionBatch(slot, actions);
        PartitionBalancer_->ProcessLsmActionBatch(slot, actions);
        StoreRotator_->ProcessLsmActionBatch(slot, actions);

        for (const auto& lsmTablet : lsmTablets) {
            auto* tablet = tabletManager->FindTablet(lsmTablet->GetId());
            if (tablet) {
                tablet->LsmStatistics() = lsmTablet->LsmStatistics();
            }
        }
    }

    void OnEndSlotScan()
    {
        StoreCompactor_->OnEndSlotScan();

        auto actions = Backend_->BuildOverallLsmActions();
        StoreRotator_->ProcessLsmActionBatch(/*slot*/ nullptr, actions);
    }

    void SetBackendState()
    {
        NLsm::TLsmBackendState backendState;

        auto timestampProvider = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetTimestampProvider();
        backendState.CurrentTimestamp = timestampProvider->GetLatestTimestamp();

        backendState.TabletNodeConfig = Bootstrap_->GetConfig()->TabletNode;
        backendState.TabletNodeDynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig()->TabletNode;

        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();
        const auto& cellar = Bootstrap_->GetCellarManager()->GetCellar(NCellarClient::ECellarType::Tablet);
        for (const auto& occupant : cellar->Occupants()) {
            if (!occupant) {
                continue;
            }

            auto occupier = occupant->GetTypedOccupier<ITabletSlot>();
            if (!occupier) {
                continue;
            }

            const auto& bundleName = occupier->GetTabletCellBundleName();
            if (backendState.Bundles.contains(bundleName)) {
                continue;
            }

            const auto& options = occupier->GetDynamicOptions();

            NLsm::TTabletCellBundleState bundleState{
                .ForcedRotationMemoryRatio = options->ForcedRotationMemoryRatio,
                .EnablePerBundleMemoryLimit = options->EnableTabletDynamicMemoryLimit,
                .DynamicMemoryLimit =
                    memoryTracker->GetLimit(EMemoryCategory::TabletDynamic, bundleName),
                .DynamicMemoryUsage =
                    memoryTracker->GetUsed(EMemoryCategory::TabletDynamic, bundleName),
            };

            backendState.Bundles[bundleName] = bundleState;
        }

        backendState.DynamicMemoryLimit = memoryTracker->GetLimit(EMemoryCategory::TabletDynamic);
        backendState.DynamicMemoryUsage = memoryTracker->GetUsed(EMemoryCategory::TabletDynamic);

        backendState.CurrentTime = TInstant::Now();

        Backend_->StartNewRound(backendState);
    }

    NLsm::TTabletPtr ScanTablet(
        const ITabletSlotPtr& slot,
        TTablet* tablet)
    {
        const auto& storeManager = tablet->GetStoreManager();

        auto lsmTablet = New<NLsm::TTablet>();
        lsmTablet->SetId(tablet->GetId());
        lsmTablet->SetCellId(slot->GetCellId());
        lsmTablet->TabletCellBundle() = slot->GetTabletCellBundleName();
        lsmTablet->SetPhysicallySorted(tablet->IsPhysicallySorted());
        lsmTablet->SetMounted(tablet->GetState() == ETabletState::Mounted);
        lsmTablet->SetMountConfig(tablet->GetSettings().MountConfig);
        lsmTablet->SetMountRevision(tablet->GetMountRevision());
        lsmTablet->SetLoggingTag(tablet->GetLoggingTag());
        lsmTablet->SetIsOutOfBandRotationRequested(tablet->GetOutOfBandRotationRequested());

        lsmTablet->SetIsForcedRotationPossible(storeManager->IsForcedRotationPossible());
        lsmTablet->SetIsOverflowRotationNeeded(storeManager->IsOverflowRotationNeeded());
        lsmTablet->SetLastPeriodicRotationTime(storeManager->GetLastPeriodicRotationTime());

        if (tablet->IsPhysicallySorted()) {
            lsmTablet->Eden() = ScanPartition(tablet->GetEden(), lsmTablet.Get());
            for (const auto& partition : tablet->PartitionList()) {
                lsmTablet->Partitions().push_back(ScanPartition(partition.get(), lsmTablet.Get()));
            }
            lsmTablet->SetOverlappingStoreCount(tablet->GetOverlappingStoreCount());
            lsmTablet->SetEdenOverlappingStoreCount(tablet->GetEdenOverlappingStoreCount());
            lsmTablet->SetCriticalPartitionCount(tablet->GetCriticalPartitionCount());
        } else {
            for (const auto& [id, store] : tablet->StoreIdMap()) {
                lsmTablet->Stores().push_back(ScanStore(store, lsmTablet.Get()));
            }
        }

        return lsmTablet;
    }

    bool AdvanceRowDigestRequestTime(TPartition* partition)
    {
        const auto& config = partition->GetTablet()->GetSettings().MountConfig;
        auto period = config->RowDigestCompaction->CheckPeriod;

        if (!period) {
            return false;
        }

        if (TInstant::Now() < partition->GetRowDigestRequestTime() + *period) {
            return false;
        }

        auto lastRequestTime = partition->GetRowDigestRequestTime();

        auto timeElapsed = Now() - lastRequestTime;
        i64 passedPeriodCount = (timeElapsed.GetValue() - 1) / period->GetValue();
        partition->SetRowDigestRequestTime(lastRequestTime + passedPeriodCount * *period);

        return true;
    }

    std::unique_ptr<NLsm::TPartition> ScanPartition(
        TPartition* partition,
        NLsm::TTablet* lsmTablet)
    {
        auto lsmPartition = std::make_unique<NLsm::TPartition>();
        lsmPartition->SetTablet(lsmTablet);
        lsmPartition->SetId(partition->GetId());
        lsmPartition->SetIndex(partition->GetIndex());
        lsmPartition->PivotKey() = partition->GetPivotKey();
        lsmPartition->NextPivotKey() = partition->GetNextPivotKey();
        lsmPartition->SetState(partition->GetState());
        lsmPartition->SetCompactionTime(partition->GetCompactionTime());
        lsmPartition->SetAllowedSplitTime(partition->GetAllowedSplitTime());
        lsmPartition->SetAllowedMergeTime(partition->GetAllowedMergeTime());
        lsmPartition->SetSamplingRequestTime(partition->GetSamplingRequestTime());
        lsmPartition->SetSamplingTime(partition->GetSamplingTime());
        lsmPartition->SetIsImmediateSplitRequested(partition->IsImmediateSplitRequested());
        lsmPartition->SetCompressedDataSize(partition->GetCompressedDataSize());
        lsmPartition->SetUncompressedDataSize(partition->GetUncompressedDataSize());

        std::vector<TStoreId> storeIdsAwaitingRowDigest;
        std::vector<TFuture<TRefCountedChunkMetaPtr>> asyncRowDigestMetas;

        // NB: We advance row digest request time even if row digests are not used to avoid
        // phasen synchronization when they are turned on.
        bool needRequestRowDigest = AdvanceRowDigestRequestTime(partition) && UseRowDigests_.load();

        if (needRequestRowDigest) {
            int chunkStoreCount = ssize(partition->Stores());
            if (partition->IsEden()) {
                chunkStoreCount -= partition->GetTablet()->GetDynamicStoreCount();
            }

            const auto& config = partition->GetTablet()->GetSettings().MountConfig;
            if (RowDigestRequestThrottler_->TryAcquire(chunkStoreCount)) {
                YT_LOG_DEBUG_IF(config->EnableLsmVerboseLogging, "Requesting row digests for partition "
                    "(TabletId: %v, PartitionId: %v)",
                    partition->GetTablet()->GetId(),
                    partition->GetId());
            } else {
                needRequestRowDigest = false;
                ThrottledRowDigestRequestCount_.Increment();
                YT_LOG_DEBUG_IF(config->EnableLsmVerboseLogging, "Row digest request for partition throttled "
                    "(TabletId: %v, PartitionId: %v)",
                    partition->GetTablet()->GetId(),
                    partition->GetId());
            }
        }

        for (const auto& store : partition->Stores()) {
            TFuture<TRefCountedChunkMetaPtr> asyncRowDigestMeta;
            lsmPartition->Stores().push_back(ScanStore(
                store,
                lsmTablet,
                needRequestRowDigest ? &asyncRowDigestMeta : nullptr));
            if (asyncRowDigestMeta) {
                storeIdsAwaitingRowDigest.push_back(store->GetId());
                asyncRowDigestMetas.push_back(std::move(asyncRowDigestMeta));
            }
        }

        if (!asyncRowDigestMetas.empty()) {
            RowDigestRequestCount_.Increment(ssize(asyncRowDigestMetas));
            AllSet(std::move(asyncRowDigestMetas))
                .SubscribeUnique(BIND(
                    &TLsmInterop::OnRowDigestMetaReceived,
                    MakeStrong(this),
                    std::move(storeIdsAwaitingRowDigest)));
        }

        return lsmPartition;
    }

    std::unique_ptr<NLsm::TStore> ScanStore(
        const IStorePtr& store,
        NLsm::TTablet* lsmTablet,
        TFuture<TRefCountedChunkMetaPtr>* asyncRowDigestMeta = nullptr)
    {
        const auto& tablet = store->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();

        auto lsmStore = std::make_unique<NLsm::TStore>();
        lsmStore->SetTablet(lsmTablet);
        lsmStore->SetId(store->GetId());
        lsmStore->SetType(store->GetType());
        lsmStore->SetStoreState(store->GetStoreState());
        lsmStore->SetCompressedDataSize(store->GetCompressedDataSize());
        lsmStore->SetUncompressedDataSize(store->GetUncompressedDataSize());
        lsmStore->SetRowCount(store->GetRowCount());
        lsmStore->SetMinTimestamp(store->GetMinTimestamp());
        lsmStore->SetMaxTimestamp(store->GetMaxTimestamp());

        if (store->IsDynamic()) {
            auto dynamicStore = store->AsDynamic();
            lsmStore->SetFlushState(dynamicStore->GetFlushState());
            lsmStore->SetLastFlushAttemptTimestamp(
                dynamicStore->GetLastFlushAttemptTimestamp());
            lsmStore->SetDynamicMemoryUsage(dynamicStore->GetDynamicMemoryUsage());
        }

        if (store->IsChunk()) {
            auto chunkStore = store->AsChunk();
            lsmStore->SetPreloadState(chunkStore->GetPreloadState());
            lsmStore->SetCompactionState(chunkStore->GetCompactionState());
            lsmStore->SetIsCompactable(storeManager->IsStoreCompactable(store));
            lsmStore->SetCreationTime(chunkStore->GetCreationTime());
            lsmStore->SetLastCompactionTimestamp(chunkStore->GetLastCompactionTimestamp());

            if (auto backingStore = chunkStore->GetBackingStore()) {
                lsmStore->SetBackingStoreMemoryUsage(backingStore->GetDynamicMemoryUsage());
            }

            // Asynchronously fetch versioned row digest meta.
            if (store->IsSorted()) {
                auto sortedChunkStore = store->AsSortedChunk();

                if (auto digest = FindRowDigestGuarded(store->GetId())) {
                    lsmStore->RowDigest() = std::move(digest);
                } else if (asyncRowDigestMeta) {
                    auto reader = sortedChunkStore
                        ->GetBackendReaders(EWorkloadCategory::SystemTabletCompaction)
                        .ChunkReader;
                    auto asyncMeta = reader->GetMeta(
                        /*options*/ {},
                        /*partitionTag*/ {},
                        std::vector<int>{TProtoExtensionTag<NTableClient::NProto::TVersionedRowDigestExt>::Value});
                    *asyncRowDigestMeta = std::move(asyncMeta);
                }
            }
        }

        if (store->IsSorted()) {
            auto sortedStore = store->AsSorted();
            lsmStore->MinKey() = sortedStore->GetMinKey();
            lsmStore->UpperBoundKey() = sortedStore->GetUpperBoundKey();
        }

        return lsmStore;
    }

    void OnRowDigestMetaReceived(
        std::vector<TStoreId> storeIds,
        TErrorOr<std::vector<TErrorOr<TRefCountedChunkMetaPtr>>> allSetResult)
    {
        YT_VERIFY(allSetResult.IsOK());
        auto errorOrRsps = allSetResult.Value();

        YT_VERIFY(storeIds.size() == errorOrRsps.size());

        for (const auto& [storeId, errorOrRsp] : Zip(storeIds, errorOrRsps)) {
            if (!errorOrRsp.IsOK()) {
                FailedRowDigestRequestCount_.Increment();
                YT_LOG_WARNING(errorOrRsp, "Failed to receive row digest for store (StoreId: %v)",
                    storeId);
                continue;
            }

            auto meta = errorOrRsp.Value();
            auto rowDigestExt = FindProtoExtension<NTableClient::NProto::TVersionedRowDigestExt>(
                meta->extensions());
            if (rowDigestExt) {
                auto guard = Guard(RowDigestCacheLock_);
                RowDigestCache_.Insert(storeId, *rowDigestExt, rowDigestExt->ByteSizeLong());
            }
        }
    }

    std::optional<TVersionedRowDigest> FindRowDigestGuarded(TStoreId storeId)
    {
        if (!UseRowDigests_.load()) {
            return {};
        }

        NTableClient::NProto::TVersionedRowDigestExt* protoDigest;
        {
            auto guard = Guard(RowDigestCacheLock_);
            protoDigest = RowDigestCache_.FindNoTouch(storeId);
        }

        if (!protoDigest) {
            return {};
        }

        NProfiling::TWallTimer timer;

        TVersionedRowDigest digest;
        FromProto(&digest, *protoDigest);

        RowDigestParseCumulativeTime_.Add(timer.GetElapsedTime());

        return std::move(digest);
    }
};

////////////////////////////////////////////////////////////////////////////////

ILsmInteropPtr CreateLsmInterop(
    IBootstrap* bootstrap,
    const IStoreCompactorPtr& storeCompactor,
    const IPartitionBalancerPtr& partitionBalancer,
    const IStoreRotatorPtr& storeRotator)
{
    return New<TLsmInterop>(bootstrap, storeCompactor, partitionBalancer, storeRotator);
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NTabletNode
