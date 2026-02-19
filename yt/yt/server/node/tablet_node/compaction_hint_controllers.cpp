#include "compaction_hint_controllers.h"
#include "compaction_hint_fetching.h"
#include "tablet.h"
#include "sorted_chunk_store.h"
#include "chunk_view_size_compaction_hint.h"
#include "row_digest_compaction_hint.h"
#include "min_hash_digest_compaction_hint.h"

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NTabletNode {

using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NTabletClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TCompactionHintConfigChange::TCompactionHintConfigChange(
    const TTableMountConfigPtr& oldConfig,
    const TTableMountConfigPtr& newConfig,
    NLsm::EStoreCompactionHintKind kind)
{
    switch (kind) {
        case NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow:
            OldEnable_ = oldConfig->EnableNarrowChunkViewCompaction;
            NewEnable_ = newConfig->EnableNarrowChunkViewCompaction;
            ConfigChanged_ = oldConfig->MaxChunkViewSizeRatio != newConfig->MaxChunkViewSizeRatio;
            break;

        case NLsm::EStoreCompactionHintKind::VersionedRowDigest:
            OldEnable_ = oldConfig->RowDigestCompaction->Enable && oldConfig->RowMergerType != ERowMergerType::Watermark;
            NewEnable_ = newConfig->RowDigestCompaction->Enable && newConfig->RowMergerType != ERowMergerType::Watermark;;
            ConfigChanged_ = oldConfig->MinDataTtl != newConfig->MinDataTtl ||
                oldConfig->MaxDataTtl != newConfig->MaxDataTtl ||
                oldConfig->MinDataVersions != newConfig->MaxDataVersions ||
                oldConfig->MaxDataVersions != newConfig->MaxDataVersions ||
                oldConfig->RowDigestCompaction->MaxObsoleteTimestampRatio != newConfig->RowDigestCompaction->MaxObsoleteTimestampRatio ||
                oldConfig->RowDigestCompaction->MaxTimestampsPerValue != newConfig->RowDigestCompaction->MaxTimestampsPerValue;
            break;

        default:
            YT_LOG_FATAL("Config change of store compaction hint is not supported (StoreCompactionHintKind: %v)",
                kind);
    }
}

TCompactionHintConfigChange::TCompactionHintConfigChange(
    const TTableMountConfigPtr& oldConfig,
    const TTableMountConfigPtr& newConfig,
    NLsm::EPartitionCompactionHintKind kind)
{
    switch (kind) {
        case NLsm::EPartitionCompactionHintKind::MinHashDigest:
            OldEnable_ = oldConfig->MinHashDigestCompaction->Enable;
            NewEnable_ = newConfig->MinHashDigestCompaction->Enable;
            ConfigChanged_ = false;
            break;

        default:
            YT_LOG_FATAL("Config change of partition compaction hint is not supported (PartitionCompactionHintKind: %v)",
                kind);
    }
}

TCompactionHintConfigChange TCompactionHintConfigChange::AsOnlyEnableConfigChange()
{
    OldEnable_ = false;
    ConfigChanged_ = false;

    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipelinePtr BuildFetchPipeline(
    TSortedChunkStore* store,
    NLsm::EStoreCompactionHintKind kind)
{
    switch (kind) {
        case NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow:
            return CreateChunkViewSizeFetchPipeline(store);

        case NLsm::EStoreCompactionHintKind::VersionedRowDigest:
            return CreateRowDigestFetchPipeline(store);

        case NLsm::EStoreCompactionHintKind::MinHashDigest:
            return CreateMinHashDigestFetchPipeline(store);

        default:
            YT_LOG_FATAL("Building fetching pipeline of store compaction hint is not supported (StoreCompactionHintKind: %v)",
                kind);
    }
}

bool DefinitelyHasNoHint(
    TSortedChunkStore* store,
    NLsm::EStoreCompactionHintKind kind)
{
    switch (kind) {
        case NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow:
            return TypeFromId(store->GetId()) != EObjectType::ChunkView;

        case NLsm::EStoreCompactionHintKind::VersionedRowDigest: {
            const auto& tableSchema = store->GetTablet()->GetTableSchema();
            return tableSchema->HasTtlColumn();
        }

        default:
            YT_LOG_FATAL("Calling |DefinitelyHasNoHint| of store compaction hint is not supported (StoreCompactionHintKind: %v)",
                kind);
    }
}

bool DefinitelyHasNoHint(
    TPartition* partition,
    NLsm::EPartitionCompactionHintKind kind)
{
    switch (kind) {
        case NLsm::EPartitionCompactionHintKind::MinHashDigest: {
            const auto& tableSchema = partition->GetTablet()->GetTableSchema();
            return tableSchema->HasAggregateColumns() || tableSchema->HasTtlColumn();
        }

        default:
            YT_LOG_FATAL("Calling |DefinitelyHasNoHint| of partition compaction hint is not supported (PartitionCompactionHintKind: %v)",
                kind);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TCompactionHintFetchPipelines::DefinitelyHasNoHint(NLsm::EStoreCompactionHintKind kind) const
{
    return !FetchPipelines_[kind].has_value();
}

void TCompactionHintFetchPipelines::OnStoreHasNoHint(NLsm::EStoreCompactionHintKind kind)
{
    YT_VERIFY(FetchPipelines_[kind].has_value());
    FetchPipelines_[kind] = std::nullopt;

    if (StoreFetchPipelines_.IsValidIndex(kind)) {
        YT_VERIFY(!StoreFetchPipelines_[kind]);
    }
    if (auto partitionKind = static_cast<NLsm::EPartitionCompactionHintKind>(kind);
        PartitionFetchPipelines_.IsValidIndex(partitionKind))
    {
        YT_VERIFY(!PartitionFetchPipelines_[partitionKind]);
    }
}

void TCompactionHintFetchPipelines::ResetStorePipeline(NLsm::EStoreCompactionHintKind kind)
{
    YT_VERIFY(StoreFetchPipelines_[kind]);
    StoreFetchPipelines_[kind].Reset();
}

void TCompactionHintFetchPipelines::ResetPartitionPipeline(NLsm::EPartitionCompactionHintKind kind)
{
    YT_VERIFY(PartitionFetchPipelines_[kind]);
    PartitionFetchPipelines_[kind].Reset();
}

void TCompactionHintFetchPipelines::InitializeStorePipeline(
    TSortedChunkStore* store,
    NLsm::EStoreCompactionHintKind kind)
{
    YT_VERIFY(!StoreFetchPipelines_[kind]);
    StoreFetchPipelines_[kind] = GetOrCreateFetchPipeline(store, kind);
}

void TCompactionHintFetchPipelines::InitializePartitionPipeline(
    TSortedChunkStore* store,
    NLsm::EPartitionCompactionHintKind kind)
{
    YT_VERIFY(!PartitionFetchPipelines_[kind]);
    PartitionFetchPipelines_[kind] = GetOrCreateFetchPipeline(store, static_cast<NLsm::EStoreCompactionHintKind>(kind));
}

TCompactionHintFetchPipelinePtr TCompactionHintFetchPipelines::GetOrCreateFetchPipeline(
    TSortedChunkStore* store,
    NLsm::EStoreCompactionHintKind kind)
{
    auto& weakPipeline = FetchPipelines_[kind];
    YT_VERIFY(weakPipeline.has_value());

    if (auto pipeline = weakPipeline->Lock()) {
        return pipeline;
    }

    auto pipeline = BuildFetchPipeline(store, kind);
    pipeline->Enqueue();
    weakPipeline = pipeline;

    return pipeline;
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, class TLsmCompactionHint, class TOwner>
TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>::TCompactionHintControllerBase(
    NLsm::EStoreCompactionHintKind storeKind,
    NLsm::EPartitionCompactionHintKind partitionKind)
    : StoreCompactionHintKind_(storeKind)
    , PartitionCompactionHintKind_(partitionKind)
{ }

template <class TDerived, class TLsmCompactionHint, class TOwner>
void TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>::OnLsmFeedbackReceived(
    TOwner* owner,
    TLsmCompactionHint&& lsmCompactionHint)
{
    YT_VERIFY(State_ == ECompactionHintState::Active);

    YT_VERIFY(LsmCompactionHint_.GetStoreCompactionHintKind() == lsmCompactionHint.GetStoreCompactionHintKind());
    YT_VERIFY(LsmCompactionHint_.GetPartitionCompactionHintKind() == lsmCompactionHint.GetPartitionCompactionHintKind());

    // Outdated feedback, skip.
    if (LsmCompactionHint_.GetNodeObjectRevision() != lsmCompactionHint.GetLsmDecisionRevision()) {
        return;
    }

    static_cast<TDerived*>(this)->ResetPipelines(owner);

    YT_VERIFY(lsmCompactionHint.IsRelevantDecisionMade());
    LsmCompactionHint_ = std::move(lsmCompactionHint);

    YT_LOG_DEBUG("Got relevant compaction hint feedback from lsm "
        "(%v, OwnerId: %v, StoreCompactionHintKind: %v, PartitionCompactionHintKind: %v)",
        owner->GetTablet()->GetLoggingTag(),
        owner->GetId(),
        StoreCompactionHintKind_,
        PartitionCompactionHintKind_);
}

template <class TDerived, class TLsmCompactionHint, class TOwner>
bool TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>::FetchInProgress() const
{
    return State_ == ECompactionHintState::Active && !LsmCompactionHint_.IsRelevantDecisionMade();
}

template <class TDerived, class TLsmCompactionHint, class TOwner>
void TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>::SetPassiveState(
    TOwner* owner,
    ECompactionHintState state)
{
    YT_VERIFY(State_ != ECompactionHintState::DefinitelyNoHint);

    if (FetchInProgress()) {
        static_cast<TDerived*>(this)->ResetPipelines(owner);
    }

    LsmCompactionHint_ = {};
    State_ = state;

    YT_LOG_DEBUG_IF(owner->GetTablet()->GetSettings().MountConfig->EnableLsmVerboseLogging,
        "Set passive state for compaction hint controller "
        "(%v, OwnerId: %v, StoreCompactionHintKind: %v, PartitionCompactionHintKind: %v, State: %v)",
        owner->GetTablet()->GetLoggingTag(),
        owner->GetId(),
        StoreCompactionHintKind_,
        PartitionCompactionHintKind_,
        State_);
}

template <class TDerived, class TLsmCompactionHint, class TOwner>
void TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>::SetActiveState(TOwner* owner)
{
    YT_VERIFY(State_ != ECompactionHintState::DefinitelyNoHint);

    // Can be used for refetch, for example if mount config was changed.
    if (!FetchInProgress()) {
        static_cast<TDerived*>(this)->InitializePipelines(owner);
    }

    LsmCompactionHint_ = {StoreCompactionHintKind_, PartitionCompactionHintKind_};
    UpdateRevision();
    State_ = ECompactionHintState::Active;

    YT_LOG_DEBUG_IF(owner->GetTablet()->GetSettings().MountConfig->EnableLsmVerboseLogging,
        "Set active state for compaction hint controller "
        "(%v, OwnerId: %v, StoreCompactionHintKind: %v, PartitionCompactionHintKind: %v)",
        owner->GetTablet()->GetLoggingTag(),
        owner->GetId(),
        StoreCompactionHintKind_,
        PartitionCompactionHintKind_);
}

template <class TDerived, class TLsmCompactionHint, class TOwner>
void TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>::SetDeterminedState(
    TOwner* owner,
    TCompactionHintConfigChange configChange,
    bool isInBadState)
{
    YT_VERIFY(State_ != ECompactionHintState::DefinitelyNoHint);

    if (!configChange.IsNewEnable()) {
        SetPassiveState(owner, ECompactionHintState::DisabledByConfig);
        return;
    }

    if (isInBadState) {
        SetPassiveState(owner, ECompactionHintState::BadState);
        return;
    }

    if (!configChange.IsOldEnable() || configChange.IsConfigChanged()) {
        SetActiveState(owner);
    }
}

template <class TDerived, class TLsmCompactionHint, class TOwner>
void TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>::UpdateRevision()
{
    auto steadyNow = std::chrono::steady_clock::now();
    auto nanosecondsFromEpoch =
        std::chrono::duration_cast<std::chrono::nanoseconds>(steadyNow.time_since_epoch()).count();

    LsmCompactionHint_.SetNodeObjectRevision(TRevision(nanosecondsFromEpoch));
}

////////////////////////////////////////////////////////////////////////////////

template class TCompactionHintControllerBase<
    TStoreCompactionHintController,
    NLsm::TStoreCompactionHint,
    TSortedChunkStore>;

template class TCompactionHintControllerBase<
    TPartitionCompactionHintController,
    NLsm::TPartitionCompactionHint,
    TPartition>;

////////////////////////////////////////////////////////////////////////////////

void TStoreCompactionHintController::StopEpoch(TSortedChunkStore* store)
{
    SetPassiveState(store, ECompactionHintState::NotInEpoch);
}

void TStoreCompactionHintController::StartEpoch(TSortedChunkStore* store)
{
    const auto& config = store->GetTablet()->GetSettings().MountConfig;
    SetDeterminedState(
        store,
        TCompactionHintConfigChange(config, config, GetStoreCompactionHintKind()).AsOnlyEnableConfigChange(),
        /*isInBadState*/ store->GetStoreState() != EStoreState::Persistent);
}

void TStoreCompactionHintController::OnMountConfigUpdated(TSortedChunkStore* store, const TTableMountConfigPtr& oldConfig)
{
    YT_VERIFY(State_ >= ECompactionHintState::DisabledByConfig);

    SetDeterminedState(
        store,
        /*configChange*/ {oldConfig, store->GetTablet()->GetSettings().MountConfig, GetStoreCompactionHintKind()},
        /*isInBadState*/ store->GetStoreState() != EStoreState::Persistent);
}

void TStoreCompactionHintController::OnStoreStateChanged(TSortedChunkStore* store)
{
    YT_VERIFY(State_ >= ECompactionHintState::BadState);

    if (store->GetStoreState() == EStoreState::Persistent) {
        SetActiveState(store);
    } else {
        SetPassiveState(store, ECompactionHintState::BadState);
    }
}

void TStoreCompactionHintController::OnStoreHasNoHint(TSortedChunkStore* store)
{
    YT_VERIFY(State_ == ECompactionHintState::Active || State_ == ECompactionHintState::NotInEpoch);
    SetPassiveState(store, ECompactionHintState::DefinitelyNoHint);
}

void TStoreCompactionHintController::ResetPipelines(TSortedChunkStore* store)
{
    YT_VERIFY(FetchInProgress());
    store->CompactionHintFetchPipelines().ResetStorePipeline(GetStoreCompactionHintKind());
}

void TStoreCompactionHintController::InitializePipelines(TSortedChunkStore* store)
{
    YT_VERIFY(!FetchInProgress());
    store->CompactionHintFetchPipelines().InitializeStorePipeline(store, GetStoreCompactionHintKind());
}

////////////////////////////////////////////////////////////////////////////////

void TStoreCompactionHints::Initialize(TSortedChunkStore* store)
{
    for (auto& controller : Controllers_) {
        if (DefinitelyHasNoHint(store, controller.GetStoreCompactionHintKind())) {
            controller.OnStoreHasNoHint(store);
        }
    }
}

void TStoreCompactionHints::StopEpoch(TSortedChunkStore* store)
{
    ForEachController(
        [store] (auto& controller) {
            controller.StopEpoch(store);
        },
        /*minState*/ ECompactionHintState::DisabledByConfig);
}

void TStoreCompactionHints::StartEpoch(TSortedChunkStore* store)
{
    ForEachController(
        [store] (auto& controller) {
            controller.StartEpoch(store);
        },
        /*minState*/ ECompactionHintState::NotInEpoch);
}

void TStoreCompactionHints::OnMountConfigUpdated(TSortedChunkStore* store, const TTableMountConfigPtr& oldConfig)
{
    ForEachController(
        [store, &oldConfig] (auto& controller) {
            controller.OnMountConfigUpdated(store, oldConfig);
        },
        /*minState*/ ECompactionHintState::DisabledByConfig);
}

void TStoreCompactionHints::OnStoreStateChanged(TSortedChunkStore* store)
{
    ForEachController(
        [store] (auto& controller) {
            controller.OnStoreStateChanged(store);
        },
        /*minState*/ ECompactionHintState::BadState);
}

void TStoreCompactionHints::OnLsmFeedbackReceived(
    TSortedChunkStore* store,
    NLsm::TCompactionHintUpdateRequest::TStoreRequest&& storeRequest)
{
    ForEachController(
        [&] (auto& controller) {
            if (auto&& hint = storeRequest.Update[controller.GetStoreCompactionHintKind()]) {
                controller.OnLsmFeedbackReceived(store, std::move(hint));
            }
        },
        /*minState*/ ECompactionHintState::Active);
}

template <class TFunction>
void TStoreCompactionHints::ForEachController(TFunction&& function, ECompactionHintState minState)
{
    for (auto& controller : Controllers_) {
        if (controller.GetState() >= minState) {
            function(controller);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionCompactionHintController::StartEpoch(TPartition* partition)
{
    const auto& config = partition->GetTablet()->GetSettings().MountConfig;
    RecalculateCounts(partition);
    SetDeterminedState(
        partition,
        TCompactionHintConfigChange(config, config, GetPartitionCompactionHintKind()).AsOnlyEnableConfigChange(),
        /*isInBadState*/ !AreAllStoresGood());
}

void TPartitionCompactionHintController::StopEpoch(TPartition* partition)
{
    SetPassiveState(partition, ECompactionHintState::NotInEpoch);
}

void TPartitionCompactionHintController::OnMountConfigUpdated(TPartition* partition, const TTableMountConfigPtr& oldConfig)
{
    YT_VERIFY(State_ >= ECompactionHintState::DisabledByConfig);

    RecalculateCounts(partition);
    SetDeterminedState(
        partition,
        /*configChange*/ {oldConfig, partition->GetTablet()->GetSettings().MountConfig, GetPartitionCompactionHintKind()},
        /*isInBadState*/ !AreAllStoresGood());
}

void TPartitionCompactionHintController::OnStoreStateChanged(TPartition* partition, TSortedChunkStore* store)
{
    YT_VERIFY(State_ >= ECompactionHintState::BadState);

    if (DefinitelyHasNoHint(store)) {
        return;
    }

    if (store->GetStoreState() == EStoreState::Persistent) {
        --BadStateStoreCount_;
        if (AreAllStoresGood()) {
            SetActiveState(partition);
        }
    } else {
        SetPassiveState(partition, ECompactionHintState::BadState);
        ++BadStateStoreCount_;
    }
}

void TPartitionCompactionHintController::OnStoreHasNoHint(TPartition* partition, TSortedChunkStore* store)
{
    YT_VERIFY(State_ >= ECompactionHintState::BadState);
    YT_VERIFY(store->GetStoreState() == EStoreState::Persistent);

    SetPassiveState(partition, ECompactionHintState::BadState);
    ++NoHintStoreCount_;
}

void TPartitionCompactionHintController::OnPartitionHasNoHint(TPartition* partition)
{
    SetPassiveState(partition, ECompactionHintState::DefinitelyNoHint);
}

void TPartitionCompactionHintController::OnStoreRemoved(TPartition* partition, TSortedChunkStore* store)
{
    YT_VERIFY(State_ >= ECompactionHintState::BadState);

    if (FetchInProgress()) {
        YT_VERIFY(AreAllStoresGood());
        store->CompactionHintFetchPipelines().ResetPartitionPipeline(GetPartitionCompactionHintKind());
    }

    if (DefinitelyHasNoHint(store)) {
        --NoHintStoreCount_;
    } else if (store->GetStoreState() != EStoreState::Persistent) {
        --BadStateStoreCount_;
    }

    if (AreAllStoresGood()) {
        SetActiveState(partition);
    }
}

void TPartitionCompactionHintController::OnStoreAdded(TPartition* partition, TSortedChunkStore* store)
{
    YT_VERIFY(State_ >= ECompactionHintState::BadState);

    if (DefinitelyHasNoHint(store)) {
        ++NoHintStoreCount_;
    } else if (store->GetStoreState() != EStoreState::Persistent) {
        ++BadStateStoreCount_;
    }

    if (!AreAllStoresGood()) {
        SetPassiveState(partition, ECompactionHintState::BadState);
        return;
    }

    if (FetchInProgress()) {
        store->CompactionHintFetchPipelines().InitializePartitionPipeline(store, GetPartitionCompactionHintKind());
        return;
    }

    SetActiveState(partition);
}

bool TPartitionCompactionHintController::AreAllStoresGood() const
{
    return NoHintStoreCount_ + BadStateStoreCount_ == 0;
}

bool TPartitionCompactionHintController::DefinitelyHasNoHint(TSortedChunkStore* store) const
{
    return store->CompactionHintFetchPipelines().DefinitelyHasNoHint(GetStoreCompactionHintKind());
}

void TPartitionCompactionHintController::RecalculateCounts(TPartition* partition)
{
    NoHintStoreCount_ = 0;
    BadStateStoreCount_ = 0;

    for (const auto& store : partition->Stores()) {
        if (DefinitelyHasNoHint(store->AsSortedChunk().Get())) {
            ++NoHintStoreCount_;
            continue;
        }

        if (store->GetStoreState() != EStoreState::Persistent) {
            ++BadStateStoreCount_;
        }
    }
}

void TPartitionCompactionHintController::ResetPipelines(TPartition* partition)
{
    YT_VERIFY(FetchInProgress());
    YT_VERIFY(AreAllStoresGood());

    for (const auto& store : partition->Stores()) {
        store->AsSortedChunk()->CompactionHintFetchPipelines().ResetPartitionPipeline(GetPartitionCompactionHintKind());
    }
}

void TPartitionCompactionHintController::InitializePipelines(TPartition* partition)
{
    YT_VERIFY(!FetchInProgress());
    YT_VERIFY(AreAllStoresGood());

    for (const auto& store : partition->Stores()) {
        auto* sortedChunkStore = store->AsSortedChunk().get();
        sortedChunkStore->CompactionHintFetchPipelines().InitializePartitionPipeline(sortedChunkStore, GetPartitionCompactionHintKind());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionCompactionHints::Initialize(TPartition* partition)
{
    for (auto& controller : Controllers_) {
        if (partition->IsEden() || DefinitelyHasNoHint(partition, controller.GetPartitionCompactionHintKind())) {
            controller.OnPartitionHasNoHint(partition);
        }
    }
}

void TPartitionCompactionHints::StartEpoch(TPartition* partition)
{
    ForEachController(
        [partition] (auto& controller) {
            controller.StartEpoch(partition);
        },
        [] (TSortedChunkStore* store) {
            store->CompactionHints().StartEpoch(store);
        },
        partition,
        ECompactionHintState::NotInEpoch);
}

void TPartitionCompactionHints::StopEpoch(TPartition* partition)
{
    ForEachController(
        [partition] (auto& controller) {
            controller.StopEpoch(partition);
        },
        [] (TSortedChunkStore* store) {
            store->CompactionHints().StopEpoch(store);
        },
        partition,
        ECompactionHintState::DisabledByConfig,
        /*onlyLeadingState*/ false);
}

void TPartitionCompactionHints::OnMountConfigUpdated(TPartition* partition, const TTableMountConfigPtr& oldConfig)
{
    ForEachController(
        [partition, &oldConfig] (auto& controller) {
            controller.OnMountConfigUpdated(partition, oldConfig);
        },
        [&oldConfig] (TSortedChunkStore* store) {
            store->CompactionHints().OnMountConfigUpdated(store, oldConfig);
        },
        partition,
        ECompactionHintState::DisabledByConfig);
}

void TPartitionCompactionHints::OnStoreStateChanged(TPartition* partition, TSortedChunkStore* store)
{
    ForEachController(
        [partition, store] (auto& controller) {
            controller.OnStoreStateChanged(partition, store);
        },
        [store] () {
            store->CompactionHints().OnStoreStateChanged(store);
        },
        partition,
        /*minState*/ ECompactionHintState::BadState);
}

void TPartitionCompactionHints::OnStoreHasNoHint(TPartition* partition, TSortedChunkStore* store, NLsm::EStoreCompactionHintKind kind)
{
    YT_VERIFY(partition->GetTablet()->GetAutomatonState() == NHydra::EPeerState::Leading);

    if (!partition->IsEden()) {
        auto partitionKind = static_cast<NLsm::EPartitionCompactionHintKind>(kind);
        if (Controllers_.IsValidIndex(partitionKind)) {
            Controllers_[partitionKind].OnStoreHasNoHint(partition, store);
        }
    }

    if (store->CompactionHints().Controllers().IsValidIndex(kind)) {
        store->CompactionHints().Controllers()[kind].OnStoreHasNoHint(store);
    }

    store->CompactionHintFetchPipelines().OnStoreHasNoHint(kind);
}

void TPartitionCompactionHints::OnStoreRemoved(TPartition* partition, TSortedChunkStore* store)
{
    ForEachController(
        [partition, store] (auto& controller) {
            controller.OnStoreRemoved(partition, store);
        },
        [store] {
            store->CompactionHints().StopEpoch(store);
        },
        partition,
        /*minState*/ ECompactionHintState::BadState);
}

void TPartitionCompactionHints::OnStoreAdded(TPartition* partition, TSortedChunkStore* store)
{
    ForEachController(
        [partition, store] (auto& controller) {
            controller.OnStoreAdded(partition, store);
        },
        [store] {
            store->CompactionHints().StartEpoch(store);
        },
        partition,
        /*minState*/ ECompactionHintState::BadState);
}

void TPartitionCompactionHints::OnLsmFeedbackReceived(
    TPartition* partition,
    NLsm::TCompactionHintUpdateRequest::TPartitionRequest&& partitionRequest)
{
    ForEachController(
        [&] (auto& controller) {
            if (auto&& hint = partitionRequest.Update[controller.GetPartitionCompactionHintKind()]) {
                controller.OnLsmFeedbackReceived(partition, std::move(hint));
            }
        },
        [&] (TSortedChunkStore* store) {
            // NB(dave11ar): Maybe rewrite.
            auto storeRequestIt = std::find_if(
                partitionRequest.StoreRequests.begin(),
                partitionRequest.StoreRequests.end(),
                [store] (const auto& storeRequest) {
                    return storeRequest.StoreId == store->GetId();
                });

            if (storeRequestIt == partitionRequest.StoreRequests.end()) {
                return;
            }

            store->CompactionHints().OnLsmFeedbackReceived(store, std::move(*storeRequestIt));
        },
        partition,
        /*minState*/ ECompactionHintState::Active);
}

template <class TPartitionFunction, class TStoreFunction>
void TPartitionCompactionHints::ForEachController(
    TPartitionFunction&& partitionFunction,
    TStoreFunction&& storeFunction,
    TPartition* partition,
    ECompactionHintState minState,
    bool onlyLeadingState)
{
    if (onlyLeadingState && partition->GetTablet()->GetAutomatonState() != NHydra::EPeerState::Leading) {
        return;
    }

    // NB(dave11ar): Partition compaction hints are always disabled in eden, but store hints still work.
    if (!partition->IsEden()) {
        for (auto& controller : Controllers_) {
            if (controller.GetState() >= minState) {
                partitionFunction(controller);
            }
        }
    }

    if constexpr (std::is_invocable_v<TStoreFunction>) {
        storeFunction();
    } else {
        for (const auto& store : partition->Stores()) {
            if (store->GetType() == EStoreType::SortedChunk) {
                storeFunction(store->AsSortedChunk().get());
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TCompactionHintFetchPipelines& CompactionHintFetchPipelines,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoMapFor(NLsm::StoreCompactionHintKinds, [&] (auto fluent, auto kinds) {
            auto storeKind = kinds.first;

            bool definitelyHasNoHint = CompactionHintFetchPipelines.DefinitelyHasNoHint(storeKind);

            TCompactionHintFetchPipelinePtr pipeline;
            if (!definitelyHasNoHint) {
                pipeline = CompactionHintFetchPipelines.FetchPipelines()[storeKind]->Lock();
            }

            bool isFetching = static_cast<bool>(pipeline);

            fluent.Item(FormatEnum(storeKind))
                .BeginMap()
                    .Item("definitely_has_no_hint").Value(definitelyHasNoHint)
                    .DoIf(!definitelyHasNoHint, [&] (auto fluent) {
                        fluent.Item("is_fetching").Value(isFetching);
                    })
                    .DoIf(isFetching, [&] (auto fluent) {
                        fluent.Item("has_payload").Value(!std::holds_alternative<std::monostate>(pipeline->Payload()));
                    })
                .EndMap();
        });
}

template <class TDerived, class TLsmCompactionHint, class TOwner>
void SerializeFragment(
    const TCompactionHintControllerBase<TDerived, TLsmCompactionHint, TOwner>& compactionHintControllerBase,
    IYsonConsumer* consumer)
{
    BuildYsonMapFragmentFluently(consumer)
        .Item("lsm_compaction_hint").Value(compactionHintControllerBase.LsmCompactionHint())
        .Item("state").Value(compactionHintControllerBase.GetState())
        .Item("store_compaction_hint_kind").Value(compactionHintControllerBase.GetStoreCompactionHintKind())
        .Item("partition_compaction_hint_kind").Value(compactionHintControllerBase.GetPartitionCompactionHintKind());
}

void Serialize(
    const TStoreCompactionHintController& storeCompactionHintController,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                SerializeFragment(
                    static_cast<const TStoreCompactionHintController::TBase>(storeCompactionHintController),
                    fluent.GetConsumer());
            })
        .EndMap();
}

void Serialize(
    const TPartitionCompactionHintController& partitionCompactionHintController,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                SerializeFragment(
                    static_cast<const TPartitionCompactionHintController::TBase>(partitionCompactionHintController),
                    fluent.GetConsumer());
            })
            .Item("no_hint_store_count").Value(partitionCompactionHintController.NoHintStoreCount_)
            .Item("bad_state_store_count").Value(partitionCompactionHintController.BadStateStoreCount_)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TStoreCompactionHints& storeCompactionHints,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoMapFor(storeCompactionHints.Controllers(), [&] (auto fluent, const auto& controller) {
            fluent.Item(FormatEnum(controller.GetStoreCompactionHintKind())).Value(controller);
        });
}

void Serialize(
    const TPartitionCompactionHints& partitionCompactionHints,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoMapFor(partitionCompactionHints.Controllers(), [&] (auto fluent, const auto& controller) {
            fluent.Item(FormatEnum(controller.GetPartitionCompactionHintKind())).Value(controller);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
