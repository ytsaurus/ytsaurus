#include "store_compactor.h"

#include "tablet.h"
#include "store.h"
#include "partition.h"

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NLsm {

using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TStoreWithReason
{
    TStore* Store;
    EStoreCompactionReason Reason = EStoreCompactionReason::None;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactor
    : public ILsmBackend
{
public:
    void StartNewRound(const TLsmBackendState& state) override
    {
        CurrentTimestamp_ = state.CurrentTimestamp;
        Config_ = state.TabletNodeConfig;
        CurrentTime_ = state.CurrentTime;
    }

    TLsmActionBatch BuildLsmActions(
        const std::vector<TTabletPtr>& tablets,
        const TString& /*bundleName*/) override
    {
        YT_LOG_DEBUG("Started building store compactor action batch");

        TLsmActionBatch batch;
        for (const auto& tablet : tablets) {
            batch.MergeWith(ScanTablet(tablet.Get()));
        }

        YT_LOG_DEBUG("Finished building store compactor action batch");

        return batch;
    }

    TLsmActionBatch BuildOverallLsmActions() override
    {
        return {};
    }

private:
    // Hydra timestamp. Crucial for consistency.
    TTimestamp CurrentTimestamp_;
    TTabletNodeConfigPtr Config_;
    // System time. Used for imprecise activities like periodic compaction.
    TInstant CurrentTime_;

    TLsmActionBatch ScanTablet(TTablet* tablet)
    {
        TLsmActionBatch batch;

        if (!tablet->IsPhysicallySorted() ||
            !tablet->GetMounted() ||
            !tablet->GetIsCompactionAllowed())
        {
            return batch;
        }

        const auto& config = tablet->GetMountConfig();
        if (!config->EnableCompactionAndPartitioning) {
            return batch;
        }

        if (config->EnablePartitioning) {
            if (auto request = ScanEdenForPartitioning(tablet->Eden().get())) {
                batch.Partitionings.push_back(std::move(*request));
            }
        }

        bool allowForcedCompaction = true;

        if (auto request = ScanPartitionForCompaction(tablet->Eden().get(), /*allowForcedCompaction*/ true)) {
            batch.Compactions.push_back(std::move(*request));
            if (request->Reason == EStoreCompactionReason::Forced &&
                config->PrioritizeEdenForcedCompaction)
            {
                allowForcedCompaction = false;
            }
        }

        for (const auto& partition : tablet->Partitions()) {
            if (auto request = ScanPartitionForCompaction(partition.get(), allowForcedCompaction)) {
                batch.Compactions.push_back(std::move(*request));
            }
        }

        return batch;
    }

    std::optional<TCompactionRequest> ScanEdenForPartitioning(TPartition* eden)
    {
        if (eden->GetState() != EPartitionState::Normal) {
            return {};
        }

        auto* tablet = eden->GetTablet();
        auto mountConfig = tablet->GetMountConfig();

        auto [reason, stores] = PickStoresForPartitioning(eden);
        if (stores.empty()) {
            return {};
        }

        // We aim to improve OSC; partitioning unconditionally improves OSC (given at least two stores).
        // So we consider how constrained is the tablet, and how many stores we consider for partitioning.
        const int overlappingStoreLimit = GetOverlappingStoreLimit(mountConfig);
        const int overlappingStoreCount = tablet->GetOverlappingStoreCount();
        const int slack = std::max(0, overlappingStoreLimit - overlappingStoreCount);
        const int effect = stores.size() - 1;

        return TCompactionRequest{
            .Tablet = MakeStrong(tablet),
            .PartitionId = eden->GetId(),
            .Stores = std::move(stores),
            .Slack = slack,
            .Effect = effect,
            .Reason = reason,
        };
    }

    std::optional<TCompactionRequest> TryDiscardExpiredPartition(TPartition* partition)
    {
        if (partition->IsEden()) {
            return {};
        }

        auto* tablet = partition->GetTablet();

        auto mountConfig = tablet->GetMountConfig();
        if (!mountConfig->EnableDiscardingExpiredPartitions ||
            mountConfig->MinDataVersions != 0 ||
            tablet->GetHasTtlColumn() ||
            mountConfig->RowMergerType == ERowMergerType::Watermark)
        {
            return {};
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetCompactionState() != EStoreCompactionState::None) {
                return {};
            }
        }

        auto partitionMaxTimestamp = NullTimestamp;
        for (const auto& store : partition->Stores()) {
            partitionMaxTimestamp = std::max(partitionMaxTimestamp, store->GetMaxTimestamp());
        }

        // NB: min_data_ttl <= max_data ttl should be validated in mount config, see YT-15160.
        auto maxDataTtl = std::max(mountConfig->MinDataTtl, mountConfig->MaxDataTtl);
        if (partitionMaxTimestamp >= CurrentTimestamp_ ||
            TimestampDiffToDuration(partitionMaxTimestamp, CurrentTimestamp_).first <= maxDataTtl)
        {
            return {};
        }

        auto majorTimestamp = CurrentTimestamp_;
        for (const auto& store : tablet->Eden()->Stores()) {
            majorTimestamp = std::min(majorTimestamp, store->GetMinTimestamp());
        }

        if (partitionMaxTimestamp >= majorTimestamp) {
            return {};
        }

        std::vector<TStoreId> stores;
        for (const auto& store : partition->Stores()) {
            stores.push_back(store->GetId());
        }

        YT_LOG_DEBUG("Found partition with expired stores (%v, PartitionId: %v, PartitionIndex: %v, "
            "PartitionMaxTimestamp: %v, MajorTimestamp: %v, StoreCount: %v)",
            tablet->GetLoggingTag(),
            partition->GetId(),
            partition->GetIndex(),
            partitionMaxTimestamp,
            majorTimestamp,
            partition->Stores().size());

        return TCompactionRequest{
            .Tablet = MakeStrong(tablet),
            .PartitionId = partition->GetId(),
            .Stores = std::move(stores),
            .DiscardStores = true,
            .Reason = EStoreCompactionReason::DiscardByTtl,
        };
    }

    std::optional<TCompactionRequest> ScanPartitionForCompaction(TPartition* partition, bool allowForcedCompaction)
    {
        if (partition->GetState() != EPartitionState::Normal ||
            partition->GetIsImmediateSplitRequested() ||
            partition->Stores().empty())
        {
            return {};
        }

        auto* tablet = partition->GetTablet();

        if (auto request = TryDiscardExpiredPartition(partition)) {
            return request;
        }

        auto [reason, stores] = PickStoresForCompaction(partition, allowForcedCompaction);
        if (stores.empty()) {
            return {};
        }

        auto request = TCompactionRequest{
            .Tablet = MakeStrong(tablet),
            .PartitionId = partition->GetId(),
            .Stores = stores,
            .Reason = reason,
        };
        auto mountConfig = tablet->GetMountConfig();
        // We aim to improve OSC; compaction improves OSC _only_ if the partition contributes towards OSC.
        // So we consider how constrained is the partition, and how many stores we consider for compaction.
        const int overlappingStoreLimit = GetOverlappingStoreLimit(mountConfig);
        const int overlappingStoreCount = tablet->GetOverlappingStoreCount();
        if (partition->IsEden()) {
            // Normalized eden store count dominates when number of eden stores is too close to its limit.
            int normalizedEdenStoreCount = tablet->Eden()->Stores().size() * overlappingStoreLimit /
                mountConfig->MaxEdenStoresPerTablet;
            int overlappingStoreLimitSlackness = overlappingStoreLimit -
                std::max(overlappingStoreCount, normalizedEdenStoreCount);

            request.Slack = std::max(0, overlappingStoreLimitSlackness);
            request.Effect = request.Stores.size() - 1;
        } else {
            // For critical partitions, this is equivalent to MOSC-OSC; for unconstrained -- includes extra slack.
            const int edenOverlappingStoreCount = tablet->GetEdenOverlappingStoreCount();
            const int partitionStoreCount = std::ssize(partition->Stores());
            request.Slack = std::max(0, overlappingStoreLimit - edenOverlappingStoreCount - partitionStoreCount);
            if (tablet->GetCriticalPartitionCount() == 1 &&
                edenOverlappingStoreCount + partitionStoreCount == overlappingStoreCount)
            {
                request.Effect = request.Stores.size() - 1;
            }
        }

        return request;
    }

    std::pair<EStoreCompactionReason, std::vector<TStoreId>>
        PickStoresForPartitioning(TPartition* eden)
    {
        std::vector<TStoreId> finalists;

        const auto* tablet = eden->GetTablet();
        auto mountConfig = tablet->GetMountConfig();

        std::vector<TStore*> candidates;

        EStoreCompactionReason finalistCompactionReason;
        for (const auto& store : eden->Stores()) {
            if (!IsStoreCompactable(store.get())) {
                continue;
            }

            auto candidate = store.get();
            candidates.push_back(candidate);

            auto compactionReason = GetStoreCompactionReason(candidate);
            if (compactionReason != EStoreCompactionReason::None) {
                finalistCompactionReason = compactionReason;
                finalists.push_back(candidate->GetId());
            }

            if (std::ssize(finalists) >= mountConfig->MaxPartitioningStoreCount) {
                break;
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            // NB: Situations when there are multiple different reasons are
            // very rare, we take arbitrary reason in this case.
            return {finalistCompactionReason, finalists};
        }

        // Sort by decreasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (const TStore* lhs, const TStore* rhs) {
                return lhs->GetCompressedDataSize() > rhs->GetCompressedDataSize();
            });

        i64 dataSizeSum = 0;
        int bestStoreCount = -1;
        for (int i = 0; i < std::ssize(candidates); ++i) {
            dataSizeSum += candidates[i]->GetCompressedDataSize();
            int storeCount = i + 1;
            if (storeCount >= mountConfig->MinPartitioningStoreCount &&
                storeCount <= mountConfig->MaxPartitioningStoreCount &&
                dataSizeSum >= mountConfig->MinPartitioningDataSize &&
                // Ignore max_partitioning_data_size limit for a minimal set of stores.
                (dataSizeSum <= mountConfig->MaxPartitioningDataSize || storeCount == mountConfig->MinPartitioningStoreCount))
            {
                // Prefer to partition more data.
                bestStoreCount = storeCount;
            }
        }

        if (bestStoreCount > 0) {
            finalists.reserve(bestStoreCount);
            for (int i = 0; i < bestStoreCount; ++i) {
                finalists.push_back(candidates[i]->GetId());
            }
        }

        return {EStoreCompactionReason::Regular, finalists};
    }

    std::pair<EStoreCompactionReason, std::vector<TStoreId>>
        PickStoresForCompaction(TPartition* partition, bool allowForcedCompaction)
    {
        std::vector<TStoreId> finalists;

        const auto* tablet = partition->GetTablet();
        auto mountConfig = tablet->GetMountConfig();

        auto Logger = NLsm::Logger().WithTag("%v, PartitionId: %v, Eden: %v",
            tablet->GetLoggingTag(),
            partition->GetId(),
            partition->IsEden());

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Picking stores for compaction");

        std::vector<TStoreWithReason> candidates;

        TEnumIndexedArray<EStoreCompactionReason, int> storeCountByReason;

        for (const auto& store : partition->Stores()) {
            if (!IsStoreCompactable(store.get())) {
                continue;
            }

            // Don't compact large Eden stores.
            if (partition->IsEden() && store->GetCompressedDataSize() >= mountConfig->MinPartitioningDataSize) {
                continue;
            }

            auto& candidate = candidates.emplace_back(TStoreWithReason{.Store = store.get()});

            auto compactionReason = GetStoreCompactionReason(candidate.Store);
            if (compactionReason == EStoreCompactionReason::None) {
                compactionReason = GetStoreCompactionReasonFromDigest(candidate.Store);
            }

            if (compactionReason != EStoreCompactionReason::None) {
                ++storeCountByReason[compactionReason];
                candidate.Reason = compactionReason;
            }
        }

        for (auto reason : TEnumTraits<EStoreCompactionReason>::GetDomainValues()) {
            if (reason == EStoreCompactionReason::None) {
                continue;
            }

            partition->GetTablet()->LsmStatistics().PendingCompactionStoreCount[reason] += storeCountByReason[reason];
        }

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging &&
                (storeCountByReason[EStoreCompactionReason::TooManyTimestamps] > 0 ||
                 storeCountByReason[EStoreCompactionReason::TtlCleanupExpected] > 0),
            "Row digest provides stores for compaction (TooManyTimestampsCount: %v, "
            "TtlCleanupExpectedCount: %v)",
            storeCountByReason[EStoreCompactionReason::TooManyTimestamps],
            storeCountByReason[EStoreCompactionReason::TtlCleanupExpected]);

        // Strictly determine order of incoming stores.
        // Use std::stable_sort instead of std::sort in the further for compaction candidates.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (const TStoreWithReason& lhs, const TStoreWithReason& rhs) {
                return lhs.Store->GetMinTimestamp() != rhs.Store->GetMinTimestamp()
                    ? lhs.Store->GetMinTimestamp() < rhs.Store->GetMinTimestamp()
                    : lhs.Store->GetId() < rhs.Store->GetId();
            });

        EStoreCompactionReason finalistCompactionReason;

        if (mountConfig->PeriodicCompactionMode == EPeriodicCompactionMode::Partition &&
            storeCountByReason[EStoreCompactionReason::Periodic] > 0)
        {
            // Check if periodic compaction for the partition has come.

            finalistCompactionReason = EStoreCompactionReason::Periodic;
            std::stable_sort(
                candidates.begin(),
                candidates.end(),
                [] (const TStoreWithReason& lhs, const TStoreWithReason& rhs) {
                    return lhs.Store->GetCreationTime() < rhs.Store->GetCreationTime();
                });

            for (auto [store, reason] : candidates) {
                finalists.push_back(store->GetId());
                if (std::ssize(finalists) >= mountConfig->MaxCompactionStoreCount) {
                    break;
                }
            }
        } else if (storeCountByReason[EStoreCompactionReason::TooManyTimestamps] > 0) {
            // Check if compaction of certain stores will likely prune some timestamps.
            // Stores are already sorted by min timestamp, so we avoid sabotage by major timestamp.

            int lastNecessaryStoreIndex = -1;
            for (int index = 0; index < std::min<int>(ssize(candidates), mountConfig->MaxCompactionStoreCount); ++index) {
                auto reason = candidates[index].Reason;
                if (reason != EStoreCompactionReason::None) {
                    finalistCompactionReason = reason;
                    lastNecessaryStoreIndex = index;
                }
            }

            if (lastNecessaryStoreIndex != -1) {
                for (int index = 0; index <= lastNecessaryStoreIndex; ++index) {
                    finalists.push_back(candidates[index].Store->GetId());
                }
            }

            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Finalist stores for compaction picked by row digest advice (StoreCount: %v, Reason: %v)",
                std::ssize(finalists),
                EStoreCompactionReason::TooManyTimestamps);
        } else if (*std::max_element(storeCountByReason.begin(), storeCountByReason.end()) > 0) {
            for (auto [store, reason] : candidates) {
                if (reason == EStoreCompactionReason::Forced && !allowForcedCompaction) {
                    continue;
                }

                if (reason != EStoreCompactionReason::None) {
                    finalistCompactionReason = reason;
                    finalists.push_back(store->GetId());
                    YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                        "Finalist store picked out of order (StoreId: %v, CompactionReason: %v)",
                        store->GetId(),
                        reason);
                }

                if (std::ssize(finalists) >= mountConfig->MaxCompactionStoreCount) {
                    break;
                }
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            // NB: Situations when there are multiple different reasons are
            // very rare, we take arbitrary reason in this case.
            return {finalistCompactionReason, std::move(finalists)};
        }

        // Sort by increasing data size.
        std::stable_sort(
            candidates.begin(),
            candidates.end(),
            [] (const TStoreWithReason& lhs, const TStoreWithReason& rhs) {
                return lhs.Store->GetCompressedDataSize() < rhs.Store->GetCompressedDataSize();
            });

        int overlappingStoreCount;
        if (partition->IsEden()) {
            overlappingStoreCount = tablet->GetOverlappingStoreCount();
        } else {
            overlappingStoreCount = partition->Stores().size() + tablet->GetEdenOverlappingStoreCount();
        }
        // Partition is critical if it contributes towards the OSC, and MOSC is reached.
        bool criticalPartition = overlappingStoreCount >= GetOverlappingStoreLimit(mountConfig);

        if (criticalPartition) {
            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Partition is critical, picking as many stores as possible");
        }

        for (int i = 0; i < std::ssize(candidates); ++i) {
            i64 dataSizeSum = 0;
            int j = i;
            while (j < std::ssize(candidates)) {
                int storeCount = j - i;
                if (storeCount > mountConfig->MaxCompactionStoreCount) {
                    break;
                }
                i64 dataSize = candidates[j].Store->GetCompressedDataSize();
                if (!criticalPartition &&
                    dataSize > mountConfig->CompactionDataSizeBase &&
                    dataSizeSum > 0 && dataSize > dataSizeSum * mountConfig->CompactionDataSizeRatio) {
                    break;
                }
                dataSizeSum += dataSize;
                ++j;
            }

            int storeCount = j - i;
            if (storeCount >= mountConfig->MinCompactionStoreCount) {
                finalists.reserve(storeCount);
                while (i < j) {
                    finalists.push_back(candidates[i].Store->GetId());
                    ++i;
                }
                YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                    "Picked stores for compaction (DataSize: %v, StoreId: %v)",
                    dataSizeSum,
                    MakeFormattableView(
                        TRange(finalists),
                        TDefaultFormatter{}));
                break;
            }
        }

        if (!finalists.empty()) {
            partition->GetTablet()->LsmStatistics().PendingCompactionStoreCount[EStoreCompactionReason::Regular] +=
                ssize(partition->Stores());
        }

        return {EStoreCompactionReason::Regular, finalists};
    }

    bool IsStoreCompactable(TStore* store)
    {
        if (!store->GetIsCompactable()) {
            return false;
        }

        return CurrentTime_ > store->GetLastCompactionTimestamp() +
            Config_->TabletManager->CompactionBackoffTime;
    }

    static bool IsStoreCompactionForced(const TStore* store)
    {
        auto mountConfig = store->GetTablet()->GetMountConfig();
        auto forcedCompactionRevision = std::max(
            mountConfig->ForcedCompactionRevision,
            mountConfig->ForcedStoreCompactionRevision);
        if (TypeFromId(store->GetId()) == EObjectType::ChunkView) {
            forcedCompactionRevision = std::max(
                forcedCompactionRevision,
                mountConfig->ForcedChunkViewCompactionRevision);
        }

        auto revision = RevisionFromId(store->GetId());
        return revision <= forcedCompactionRevision.value_or(NHydra::NullRevision);
    }

    static bool IsStoreGlobalCompactionNeeded(const TStore* store)
    {
        const auto& globalConfig = store->GetTablet()->GetMountConfig()->GlobalCompaction;

        auto now = TInstant::Now();

        if (now < globalConfig.StartTime || globalConfig.StartTime < store->GetCreationTime()) {
            return false;
        }

        if (now >= globalConfig.StartTime + globalConfig.Duration) {
            YT_LOG_DEBUG("Found store that was supposed to be compacted by now (%v, StoreId: %v, StartTime: %v, Duration: %v, Now: %v)",
                store->GetTablet()->GetLoggingTag(),
                store->GetId(),
                globalConfig.StartTime,
                globalConfig.Duration,
                now);

            return true;
        }

        double hash = ComputeHash(store->GetId());

        return globalConfig.StartTime + globalConfig.Duration * (hash / std::numeric_limits<size_t>::max()) <= now;
    }

    bool IsStorePeriodicCompactionNeeded(const TStore* store) const
    {
        auto mountConfig = store->GetTablet()->GetMountConfig();
        if (!mountConfig->AutoCompactionPeriod) {
            return false;
        }

        auto splayRatio = mountConfig->AutoCompactionPeriodSplayRatio *
            store->GetId().Parts32[0] / std::numeric_limits<ui32>::max();
        auto effectivePeriod = *mountConfig->AutoCompactionPeriod * (1 + splayRatio);
        if (CurrentTime_ < store->GetCreationTime() + effectivePeriod) {
            return false;
        }

        return true;
    }

    static bool IsStoreOutOfTabletRange(const TStore* store)
    {
        const auto* tablet = store->GetTablet();
        if (store->MinKey() < tablet->Partitions().front()->PivotKey()) {
            return true;
        }

        if (store->UpperBoundKey() > tablet->Partitions().back()->NextPivotKey()) {
            return true;
        }

        return false;
    }

    EStoreCompactionReason GetStoreCompactionReasonFromDigest(const TStore* store) const
    {
        const auto& rowDigest = store->CompactionHints().RowDigest;
        return rowDigest.Reason != EStoreCompactionReason::None && CurrentTime_ >= rowDigest.Timestamp
            ? rowDigest.Reason
            : EStoreCompactionReason::None;
    }

    EStoreCompactionReason GetStoreCompactionReason(const TStore* store) const
    {
        if (IsStoreCompactionForced(store)) {
            return EStoreCompactionReason::Forced;
        }

        if (IsStoreGlobalCompactionNeeded(store)) {
            return EStoreCompactionReason::Global;
        }

        if (IsStorePeriodicCompactionNeeded(store)) {
            return EStoreCompactionReason::Periodic;
        }

        if (IsStoreOutOfTabletRange(store)) {
            return EStoreCompactionReason::StoreOutOfTabletRange;
        }

        if (store->CompactionHints().IsChunkViewTooNarrow) {
            return EStoreCompactionReason::NarrowChunkView;
        }

        return EStoreCompactionReason::None;
    }

    static int GetOverlappingStoreLimit(const TTableMountConfigPtr& config)
    {
        return std::min(
            config->MaxOverlappingStoreCount,
            config->CriticalOverlappingStoreCount.value_or(config->MaxOverlappingStoreCount));
    }
};

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreateStoreCompactor()
{
    return New<TStoreCompactor>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
