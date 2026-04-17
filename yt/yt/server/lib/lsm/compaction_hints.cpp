#include "compaction_hints.h"
#include "tablet.h"
#include "lsm_backend.h"

#include <yt/yt/server/lib/tablet_node/private.h>
#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/table_client/versioned_row_digest.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NLsm {

using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

bool TCompactionHintBase::TCompactionHintRecalculationFinalizerBase::TryApplyRecalculation(
    TInstant timestamp,
    EStoreCompactionReason reason)
{
    YT_VERIFY(reason != EStoreCompactionReason::None);

    if (Reason_ == EStoreCompactionReason::None || timestamp < Timestamp_) {
        Reason_ = reason;
        Timestamp_ = timestamp;

        return true;
    }

    return false;
}

TCompactionHintBase::TCompactionHintBase(
    EStoreCompactionHintKind storeCompactionHintKind,
    EPartitionCompactionHintKind partitionCompactionHintKind)
    : StoreCompactionHintKind_(storeCompactionHintKind)
    , PartitionCompactionHintKind_(partitionCompactionHintKind)
{ }

TCompactionHintBase::operator bool() const
{
    return StoreCompactionHintKind_ != EStoreCompactionHintKind::None;
}

bool TCompactionHintBase::IsRelevantLsmResponse() const
{
    return NodeObjectRevision_ == LsmResponseRevision_;
}

bool TCompactionHintBase::IsSuitableTimeForCompaction(TInstant currentTime) const
{
    return IsRelevantLsmResponse() &&
        Reason_ != EStoreCompactionReason::None &&
        currentTime >= Timestamp_;
}

void TCompactionHintBase::ApplyRecalculation(TInstant timestamp, EStoreCompactionReason reason)
{
    // Revisions are not null and currect response is outdated.
    YT_VERIFY(LsmResponseRevision_ < NodeObjectRevision_);

    LsmResponseRevision_ = NodeObjectRevision_;
    Timestamp_ = timestamp;
    Reason_ = reason;
}

template <class TRecalculator>
bool TCompactionHintBase::DoRecalculateHint(TRecalculator&& recalculator, TRange<std::unique_ptr<TStore>> stores)
{
    if (IsRelevantLsmResponse()) {
        return false;
    }

    for (const auto& store : stores) {
        if (std::holds_alternative<std::monostate>(store->CompactionHints().Payloads()[StoreCompactionHintKind_])) {
            return false;
        }
    }

    std::forward<TRecalculator>(recalculator)();

    // Recalculator should make relevant response.
    YT_VERIFY(IsRelevantLsmResponse());

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TStoreCompactionHint::TStoreCompactionHintRecalculationFinalizer::TStoreCompactionHintRecalculationFinalizer(
    TStore* store,
    TStoreCompactionHint* hint)
    : Store_(store)
    , Hint_(hint)
{ }

TStoreCompactionHint::TStoreCompactionHintRecalculationFinalizer::~TStoreCompactionHintRecalculationFinalizer()
{
    Hint_->ApplyRecalculation(Timestamp_, Reason_);
}

bool TStoreCompactionHint::TStoreCompactionHintRecalculationFinalizer::TryApplyRecalculation(
    TInstant timestamp,
    EStoreCompactionReason reason)
{
    YT_LOG_DEBUG("Candidate store compaction hint lsm response provided "
        "(%v, StoreId: %v, StoreCompactionHintKind: %v, Timestamp: %v, Reason: %v)",
        Store_->GetTablet()->GetLoggingTag(),
        Store_->GetId(),
        Hint_->StoreCompactionHintKind_,
        timestamp,
        reason);

    return TCompactionHintRecalculationFinalizerBase::TryApplyRecalculation(timestamp, reason);
}

bool TStoreCompactionHint::RecalculateHint(const std::unique_ptr<TStore>& store)
{
    auto doRecalculate = [&] <EStoreCompactionHintKind Kind> {
        bool recalculated = DoRecalculateHint(
            std::bind_front(DoRecalculateStoreCompactionHint<Kind>, store.get()),
            {&store, 1});

        YT_LOG_DEBUG_IF(recalculated,
            "Store compaction hint lsm response was made "
            "(%v, StoreId: %v, StoreCompactionHintKind: %v, Timestamp: %v, Reason: %v, Revision: %v)",
            store->GetTablet()->GetLoggingTag(),
            store->GetId(),
            StoreCompactionHintKind_,
            Timestamp_,
            Reason_,
            LsmResponseRevision_);

        return recalculated;
    };

    switch (StoreCompactionHintKind_) {
        case EStoreCompactionHintKind::ChunkViewTooNarrow:
            return doRecalculate.operator()<EStoreCompactionHintKind::ChunkViewTooNarrow>();

        case EStoreCompactionHintKind::VersionedRowDigest:
            return doRecalculate.operator()<EStoreCompactionHintKind::VersionedRowDigest>();

        default:
            YT_LOG_FATAL("Recalculation of store compaction hint is not supported (StoreCompactionHintKind: %v)",
                StoreCompactionHintKind_);
    }
}

TStoreCompactionHint::TStoreCompactionHintRecalculationFinalizer TStoreCompactionHint::BuildRecalculationFinalizer(TStore* store)
{
    return TStoreCompactionHintRecalculationFinalizer(store, this);
}

////////////////////////////////////////////////////////////////////////////////

EStoreCompactionReason TStoreCompactionHints::GetStoreCompactionReason(TInstant currentTime) const
{
    for (const auto& hint : Hints_) {
        if (hint.IsSuitableTimeForCompaction(currentTime)) {
            return hint.GetReason();
        }
    }

    return EStoreCompactionReason::None;
}

bool TStoreCompactionHints::RecalculateHints(const std::unique_ptr<TStore>& store)
{
    bool recalculated = false;
    for (auto& hint : Hints_) {
        if (hint) {
            recalculated |= hint.RecalculateHint(store);
        }
    }

    return recalculated;
}

////////////////////////////////////////////////////////////////////////////////

TPartitionCompactionHint::TPartitionCompactionHintRecalculationFinalizer::TPartitionCompactionHintRecalculationFinalizer(
    TPartition* partition,
    TPartitionCompactionHint* hint)
    : Partition_(partition)
    , Hint_(hint)
{
    Stores_.reserve(partition->Stores().size());
    for (const auto& store : partition->Stores()) {
        Stores_.push_back(store.get());
    }

    std::sort(Stores_.begin(), Stores_.end(), [] (TStore* lhs, TStore* rhs) {
        return lhs->GetMinTimestamp() < rhs->GetMinTimestamp();
    });
}

TPartitionCompactionHint::TPartitionCompactionHintRecalculationFinalizer::~TPartitionCompactionHintRecalculationFinalizer()
{
    Hint_->ApplyRecalculation(Timestamp_, Reason_, GetStoreIds());
}

void TPartitionCompactionHint::TPartitionCompactionHintRecalculationFinalizer::TryApplyRecalculationByPrefix(
    TInstant timestamp,
    EStoreCompactionReason reason,
    int storePrefixLength)
{
    YT_VERIFY(storePrefixLength < std::numeric_limits<ui64>::digits);
    TryApplyRecalculationBySubset(timestamp, reason, (1ULL << storePrefixLength) - 1);
}

void TPartitionCompactionHint::TPartitionCompactionHintRecalculationFinalizer::TryApplyRecalculationBySubset(
    TInstant timestamp,
    EStoreCompactionReason reason,
    ui64 storeSubset)
{
    YT_LOG_DEBUG("Candidate partition compaction hint lsm response provided "
        "(%v, PartitionId: %v, PartitionCompactionHintKind: %v, Timestamp: %v, Reason: %v, StoreSubset: %v)",
        Partition_->GetTablet()->GetLoggingTag(),
        Partition_->GetId(),
        Hint_->StoreCompactionHintKind_,
        timestamp,
        reason,
        storeSubset);

    if (TCompactionHintRecalculationFinalizerBase::TryApplyRecalculation(timestamp, reason)) {
        StoreSubset_ = storeSubset;
    }
}

bool TPartitionCompactionHint::TPartitionCompactionHintRecalculationFinalizer::StoreSubsetContains(int index) const
{
    return (StoreSubset_ & (1ULL << index)) != 0;
}

std::vector<TStoreId> TPartitionCompactionHint::TPartitionCompactionHintRecalculationFinalizer::GetStoreIds() const
{
    if (StoreSubset_ == 0) {
        return {};
    }

    std::vector<TStoreId> storeIds;
    storeIds.reserve(std::popcount(StoreSubset_));
    for (ui32 index = 0; index < ssize(Stores_); ++index) {
        if (StoreSubsetContains(index)) {
            storeIds.push_back(Stores_[index]->GetId());
        }
    }

    return storeIds;
}

bool TPartitionCompactionHint::RecalculateHint(TPartition* partition)
{
    auto doRecalculate = [&] <EPartitionCompactionHintKind Kind> {
        bool recalculated = DoRecalculateHint(
            std::bind_front(DoRecalculatePartitionCompactionHint<Kind>, partition),
            partition->Stores());

        YT_LOG_DEBUG_IF(recalculated,
            "Partition compaction hint lsm response was made "
            "(%v, PartitionId: %v, PartitionCompactionHintKind: %v, Timestamp: %v, Reason: %v, Revision: %v, StoreIds: %v)",
            partition->GetTablet()->GetLoggingTag(),
            partition->GetId(),
            PartitionCompactionHintKind_,
            Timestamp_,
            Reason_,
            LsmResponseRevision_,
            StoreIds_);

        return recalculated;
    };

    switch (PartitionCompactionHintKind_) {
        case NLsm::EPartitionCompactionHintKind::AggregateVersionedRowDigest:
            return doRecalculate.operator()<EPartitionCompactionHintKind::AggregateVersionedRowDigest>();

        case EPartitionCompactionHintKind::MinHashDigest:
            return doRecalculate.operator()<EPartitionCompactionHintKind::MinHashDigest>();

        default:
            YT_LOG_FATAL("Recalculation of partition compaction hint is not supported (PartitionCompactionHintKind: %v)",
                PartitionCompactionHintKind_);
    }
}

TPartitionCompactionHint::TPartitionCompactionHintRecalculationFinalizer TPartitionCompactionHint::BuildRecalculationFinalizer(
    TPartition* partition)
{
    return TPartitionCompactionHintRecalculationFinalizer(partition, this);
}

void TPartitionCompactionHint::ApplyRecalculation(TInstant timestamp, EStoreCompactionReason reason, std::vector<TStoreId>&& storeIds)
{
    StoreIds_ = std::move(storeIds);
    TCompactionHintBase::ApplyRecalculation(timestamp, reason);
}

////////////////////////////////////////////////////////////////////////////////

TPartitionCompactionHints::TPartitionCompactionHints(THints hints)
    : Hints_(std::move(hints))
{ }

std::pair<EStoreCompactionReason, std::vector<TStoreId>> TPartitionCompactionHints::GetStoresForCompaction(
    TInstant currentTime,
    TTimestamp edenMajorTimestamp,
    const TTableMountConfigPtr& mountConfig) const
{
    auto edenMajorTimestampInstant = TimestampToInstant(edenMajorTimestamp).first;

    for (const auto& hint : Hints_) {
        if (!IsCompactionAllowed(hint, currentTime, edenMajorTimestampInstant, mountConfig)) {
            continue;
        }

        YT_VERIFY(!hint.StoreIds().empty());

        return {hint.GetReason(), hint.StoreIds()};
    }

    return {EStoreCompactionReason::None, {}};
}

bool TPartitionCompactionHints::RecalculateHints(TPartition* partition)
{
    bool recalculated = false;
    for (auto& hint : Hints_) {
        if (hint) {
            recalculated |= hint.RecalculateHint(partition);
        }
    }

    return recalculated;
}


bool TPartitionCompactionHints::IsCompactionAllowed(
    const TPartitionCompactionHint& hint,
    TInstant currentTime,
    TInstant edenMajorTimestampInstant,
    const TTableMountConfigPtr& mountConfig)
{
    if (!hint || !hint.IsSuitableTimeForCompaction(currentTime)) {
        return false;
    }

    TDuration dataOffset;
    switch (hint.GetReason()) {
        case EStoreCompactionReason::AggregateTtlCleanupExpected:
            dataOffset = mountConfig->MinDataVersions == 0
                ? (mountConfig->MaxDataVersions == 0
                    ? mountConfig->MinDataTtl
                    : mountConfig->MaxDataTtl)
                : mountConfig->MinDataTtl;
            break;

        case EStoreCompactionReason::AggregateDeleteTooManyTimestamps:
        case EStoreCompactionReason::RemoveDuplicates:
        case EStoreCompactionReason::ApplyDeletions:
            dataOffset = mountConfig->MinDataTtl;
            break;

        default:
            YT_LOG_FATAL("Unknown compaction reason for partition compaction hint (Reason: %v)",
                hint.GetReason());
    }

    // Latest timestamp among the data that will be deleted.
    auto dataTimestamp = hint.GetTimestamp() - dataOffset;

    return dataTimestamp < edenMajorTimestampInstant;
}

////////////////////////////////////////////////////////////////////////////////

TCompactionHintUpdateRequest RecalculateCompactionHints(TTablet* tablet)
{
    TCompactionHintUpdateRequest tabletRequest{
        .CellId = tablet->GetCellId(),
        .TabletId = tablet->GetId(),
    };

    for (const auto& partition : tablet->Partitions()) {
        bool hasUpdate = false;
        TCompactionHintUpdateRequest::TPartitionRequest partitionRequest{.PartitionId = partition->GetId()};

        // Calculate store compaction hints.
        for (const auto& store : partition->Stores()) {
            if (!store->CompactionHints().RecalculateHints(store)) {
                continue;
            }

            hasUpdate = true;
            partitionRequest.StoreRequests.push_back({
                .StoreId = store->GetId(),
                .Update = store->CompactionHints().Hints(),
            });
        }

        // Calculate partition compaction hints.
        if (partition->CompactionHints().RecalculateHints(partition.get())) {
            hasUpdate = true;
            partitionRequest.Update = partition->CompactionHints().Hints();
        }

        if (hasUpdate) {
            tabletRequest.PartitionRequests.push_back(std::move(partitionRequest));
        }
    }

    return tabletRequest;
}

////////////////////////////////////////////////////////////////////////////////

void SerializeFragment(
    const TCompactionHintBase& compactionHintBase,
    IYsonConsumer* consumer)
{
    BuildYsonMapFragmentFluently(consumer)
        .Item("node_object_revision").Value(compactionHintBase.GetNodeObjectRevision())
        .Item("lsm_response_revision").Value(compactionHintBase.GetLsmResponseRevision())
        .Item("timestamp").Value(compactionHintBase.GetTimestamp())
        .Item("reason").Value(compactionHintBase.GetReason())
        .Item("store_compaction_hint_kind").Value(compactionHintBase.GetStoreCompactionHintKind())
        .Item("partition_compaction_hint_kind").Value(compactionHintBase.GetPartitionCompactionHintKind());
}

void Serialize(
    const TStoreCompactionHint& storeCompactionHint,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                SerializeFragment(static_cast<const TCompactionHintBase&>(storeCompactionHint), fluent.GetConsumer());
            })
        .EndMap();
}

void Serialize(
    const TPartitionCompactionHint& partitionCompactionHint,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                SerializeFragment(static_cast<const TCompactionHintBase&>(partitionCompactionHint), fluent.GetConsumer());
            })
            .Item("store_ids").Value(partitionCompactionHint.StoreIds())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
