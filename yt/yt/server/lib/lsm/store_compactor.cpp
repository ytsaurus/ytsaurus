#include "store_compactor.h"

#include "tablet.h"
#include "store.h"
#include "partition.h"

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NLsm {

using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NTabletNode;

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactor
    : public ILsmBackend
{
public:
    virtual void StartNewRound(const TLsmBackendState& state) override
    {
        CurrentTimestamp_ = state.CurrentTimestamp;
        Config_ = state.TabletNodeConfig;
    }

    virtual TLsmActionBatch BuildLsmActions(
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

    virtual TLsmActionBatch BuildOverallLsmActions() override
    {
        return {};
    }

private:
    TTimestamp CurrentTimestamp_;
    TTabletNodeConfigPtr Config_;

    TLsmActionBatch ScanTablet(TTablet* tablet)
    {
        TLsmActionBatch batch;

        if (!tablet->IsPhysicallySorted() || !tablet->GetMounted()) {
            return batch;
        }

        const auto& config = tablet->GetMountConfig();
        if (!config->EnableCompactionAndPartitioning) {
            return batch;
        }

        if (auto request = ScanEdenForPartitioning(tablet->Eden().get())) {
            batch.Partitionings.push_back(std::move(*request));
        }
        if (auto request = ScanPartitionForCompaction(tablet->Eden().get())) {
            batch.Compactions.push_back(std::move(*request));
        }

        for (const auto& partition : tablet->Partitions()) {
            if (auto request = ScanPartitionForCompaction(partition.get())) {
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

        auto stores = PickStoresForPartitioning(eden);
        if (stores.empty()) {
            return {};
        }

        const auto& mountConfig = tablet->GetMountConfig();
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
        };
    }

    std::optional<TCompactionRequest> TryDiscardExpiredPartition(TPartition* partition)
    {
        if (partition->IsEden()) {
            return {};
        }

        auto* tablet = partition->GetTablet();

        const auto& mountConfig = tablet->GetMountConfig();
        if (!mountConfig->EnableDiscardingExpiredPartitions || mountConfig->MinDataVersions != 0) {
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

        return TCompactionRequest{
            .Tablet = MakeStrong(tablet),
            .PartitionId = partition->GetId(),
            .Stores = std::move(stores),
            .DiscardStores = true,
        };

        YT_LOG_DEBUG("Found partition with expired stores (%v, PartitionId: %v, PartitionIndex: %v, "
            "PartitionMaxTimestamp: %v, MajorTimestamp: %v, StoreCount: %v)",
            tablet->GetLoggingTag(),
            partition->GetId(),
            partition->GetIndex(),
            partitionMaxTimestamp,
            majorTimestamp,
            partition->Stores().size());
    }

    std::optional<TCompactionRequest> ScanPartitionForCompaction(TPartition* partition)
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


        auto stores = PickStoresForCompaction(partition);
        if (stores.empty()) {
            return {};
        }

        auto request = TCompactionRequest{
            .Tablet = MakeStrong(tablet),
            .PartitionId = partition->GetId(),
            .Stores = stores,
        };
        const auto& mountConfig = tablet->GetMountConfig();
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
            const int partitionStoreCount = static_cast<int>(partition->Stores().size());
            request.Slack = std::max(0, overlappingStoreLimit - edenOverlappingStoreCount - partitionStoreCount);
            if (tablet->GetCriticalPartitionCount() == 1 &&
                edenOverlappingStoreCount + partitionStoreCount == overlappingStoreCount)
            {
                request.Effect = request.Stores.size() - 1;
            }
        }

        return request;
    }

    std::vector<TStoreId> PickStoresForPartitioning(TPartition* eden)
    {
        std::vector<TStoreId> finalists;

        const auto* tablet = eden->GetTablet();
        const auto& mountConfig = tablet->GetMountConfig();

        std::vector<TStore*> candidates;

        for (const auto& store : eden->Stores()) {
            if (!IsStoreCompactable(store.get())) {
                continue;
            }

            auto candidate = store.get();
            candidates.push_back(candidate);

            auto compactionReason = GetStoreCompactionReason(candidate);
            if (compactionReason != EStoreCompactionReason::None) {
                finalists.push_back(candidate->GetId());
            }

            if (std::ssize(finalists) >= mountConfig->MaxPartitioningStoreCount) {
                break;
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            return finalists;
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

        return finalists;
    }

    std::vector<TStoreId> PickStoresForCompaction(TPartition* partition)
    {
        std::vector<TStoreId> finalists;

        const auto* tablet = partition->GetTablet();
        const auto& mountConfig = tablet->GetMountConfig();

        auto Logger = NLsm::Logger.WithTag("%v, PartitionId: %v",
            tablet->GetLoggingTag(),
            partition->GetId());

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Picking stores for compaction");

        std::vector<TStore*> candidates;

        TEnumIndexedVector<EStoreCompactionReason, int> storeCountByReason;

        for (const auto& store : partition->Stores()) {
            if (!IsStoreCompactable(store.get())) {
                continue;
            }

            // Don't compact large Eden stores.
            if (partition->IsEden() && store->GetCompressedDataSize() >= mountConfig->MinPartitioningDataSize) {
                continue;
            }

            auto candidate = store.get();
            candidates.push_back(candidate);

            auto compactionReason = GetStoreCompactionReason(candidate);
            if (compactionReason != EStoreCompactionReason::None) {
                ++storeCountByReason[compactionReason];
            }
        }

        // Check if periodic compaction for the partition has come.
        if (mountConfig->PeriodicCompactionMode == EPeriodicCompactionMode::Partition &&
            storeCountByReason[EStoreCompactionReason::PeriodicCompaction] > 0)
        {
            std::sort(
                candidates.begin(),
                candidates.end(),
                [] (auto* lhs, auto* rhs) {
                    return lhs->GetCreationTime() < rhs->GetCreationTime();
                });

            for (auto* candidate : candidates) {
                finalists.push_back(candidate->GetId());
                if (std::ssize(finalists) >= mountConfig->MaxCompactionStoreCount) {
                    break;
                }
            }
        } else if (*std::max_element(storeCountByReason.begin(), storeCountByReason.end()) > 0) {
            for (auto* candidate : candidates) {
                auto compactionReason = GetStoreCompactionReason(candidate);
                if (compactionReason != EStoreCompactionReason::None) {
                    finalists.push_back(candidate->GetId());
                    YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                        "Finalist store picked out of order (StoreId: %v, CompactionReason: %v)",
                        candidate->GetId(),
                        compactionReason);
                }

                if (std::ssize(finalists) >= mountConfig->MaxCompactionStoreCount) {
                    break;
                }
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            return finalists;
        }

        // Sort by increasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (const TStore* lhs, const TStore* rhs) {
                return lhs->GetCompressedDataSize() < rhs->GetCompressedDataSize();
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
                i64 dataSize = candidates[j]->GetCompressedDataSize();
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
                    finalists.push_back(candidates[i]->GetId());
                    ++i;
                }
                YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                    "Picked stores for compaction (DataSize: %v, StoreId: %v)",
                    dataSizeSum,
                    MakeFormattableView(
                        MakeRange(finalists),
                        TDefaultFormatter{}));
                break;
            }
        }

        return finalists;
    }

    bool IsStoreCompactable(TStore* store)
    {
        if (!store->GetIsCompactable()) {
            return false;
        }

        return Now() > store->GetLastCompactionTimestamp() +
            Config_->TabletManager->CompactionBackoffTime;
    }

    static bool IsStoreCompactionForced(const TStore* store)
    {
        const auto& mountConfig = store->GetTablet()->GetMountConfig();
        auto forcedCompactionRevision = std::max(
            mountConfig->ForcedCompactionRevision,
            mountConfig->ForcedStoreCompactionRevision);
        if (TypeFromId(store->GetId()) == EObjectType::ChunkView) {
            forcedCompactionRevision = std::max(
                forcedCompactionRevision,
                mountConfig->ForcedChunkViewCompactionRevision);
        }

        auto revision = CounterFromId(store->GetId());
        return revision <= forcedCompactionRevision.value_or(NHydra::NullRevision);
    }

    static bool IsStorePeriodicCompactionNeeded(const TStore* store)
    {
        const auto& mountConfig = store->GetTablet()->GetMountConfig();
        if (!mountConfig->AutoCompactionPeriod) {
            return false;
        }

        auto splayRatio = mountConfig->AutoCompactionPeriodSplayRatio *
            store->GetId().Parts32[0] / std::numeric_limits<ui32>::max();
        auto effectivePeriod = *mountConfig->AutoCompactionPeriod * (1 + splayRatio);
        if (TInstant::Now() < store->GetCreationTime() + effectivePeriod) {
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

    static EStoreCompactionReason GetStoreCompactionReason(const TStore* store)
    {
        if (IsStoreCompactionForced(store)) {
            return EStoreCompactionReason::ForcedCompaction;
        }

        if (IsStorePeriodicCompactionNeeded(store)) {
            return EStoreCompactionReason::PeriodicCompaction;
        }

        if (IsStoreOutOfTabletRange(store)) {
            return EStoreCompactionReason::StoreOutOfTabletRange;
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
