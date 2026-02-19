#include "compaction_hints.h"
#include "tablet.h"
#include "lsm_backend.h"

#include <yt/yt/server/lib/tablet_node/private.h>
#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/table_client/versioned_row_digest.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NLsm {

using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

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

bool TCompactionHintBase::IsRelevantDecisionMade() const
{
    return NodeObjectRevision_ == LsmDecisionRevision_;
}

bool TCompactionHintBase::IsSuitableTimeForCompaction(TInstant currentTime) const
{
    return IsRelevantDecisionMade() &&
        Reason_ != EStoreCompactionReason::None &&
        currentTime >= Timestamp_;
}

void TCompactionHintBase::MakeDecision(TInstant timestamp, EStoreCompactionReason reason)
{
    // Revisions are not null and currect decision is outdated.
    YT_VERIFY(LsmDecisionRevision_ < NodeObjectRevision_);

    LsmDecisionRevision_ = NodeObjectRevision_;
    Timestamp_ = timestamp;
    Reason_ = reason;
}

template <class TRecalculator>
bool TCompactionHintBase::DoRecalculateHint(TRecalculator&& recalculator, TRange<std::unique_ptr<TStore>> stores)
{
    if (IsRelevantDecisionMade()) {
        return false;
    }

    for (const auto& store : stores) {
        if (std::holds_alternative<std::monostate>(store->CompactionHints().Payloads()[StoreCompactionHintKind_])) {
            return false;
        }
    }

    std::forward<TRecalculator>(recalculator)();

    // Recalculator should make decision.
    YT_VERIFY(IsRelevantDecisionMade());

    return true;
}

////////////////////////////////////////////////////////////////////////////////

bool TStoreCompactionHint::RecalculateHint(const std::unique_ptr<TStore>& store)
{
    auto doRecalculate = [&] <EStoreCompactionHintKind Kind> {
        bool recalculated = DoRecalculateHint(
            std::bind_front(DoRecalculateStoreCompactionHint<Kind>, store.get()),
            {&store, 1});

        YT_LOG_DEBUG_IF(recalculated,
            "Store compaction hint decision was made "
            "(%v, StoreId: %v, StoreCompactionHintKind: %v, Timestamp: %v, Reason: %v, Revision: %v)",
            store->GetTablet()->GetLoggingTag(),
            store->GetId(),
            StoreCompactionHintKind_,
            Timestamp_,
            Reason_,
            LsmDecisionRevision_);

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

bool TPartitionCompactionHint::RecalculateHint(TPartition* partition)
{
    auto doRecalculate = [&] <EPartitionCompactionHintKind Kind> {
        bool recalculated = DoRecalculateHint(
            std::bind_front(DoRecalculatePartitionCompactionHint<Kind>, partition),
            partition->Stores());

        YT_LOG_DEBUG_IF(recalculated,
            "Partition compaction hint decision was made "
            "(%v, PartitionId: %v, PartitionCompactionHintKind: %v, Timestamp: %v, Reason: %v, Revision: %v, StoreIds: %v)",
            partition->GetTablet()->GetLoggingTag(),
            partition->GetId(),
            PartitionCompactionHintKind_,
            Timestamp_,
            Reason_,
            LsmDecisionRevision_,
            StoreIds_);

        return recalculated;
    };

    switch (PartitionCompactionHintKind_) {
        case EPartitionCompactionHintKind::MinHashDigest:
            return doRecalculate.operator()<EPartitionCompactionHintKind::MinHashDigest>();

        default:
            YT_LOG_FATAL("Recalculation of partition compaction hint is not supported (PartitionCompactionHintKind: %v)",
                PartitionCompactionHintKind_);
    }
}

////////////////////////////////////////////////////////////////////////////////

TPartitionCompactionHints::TPartitionCompactionHints(THints hints)
    : Hints_(std::move(hints))
{ }

std::pair<EStoreCompactionReason, std::vector<TStoreId>> TPartitionCompactionHints::GetStoresForCompaction(
    TInstant currentTime,
    TTimestamp edenMajorTimestamp) const
{
    auto edenMajorInstant = TimestampToInstant(edenMajorTimestamp).first;

    for (const auto& hint : Hints_) {
        if (!hint ||
            !hint.IsSuitableTimeForCompaction(currentTime) ||
            hint.GetTimestamp() >= edenMajorInstant)
        {
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
        .Item("lsm_decision_revision").Value(compactionHintBase.GetLsmDecisionRevision())
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
