#pragma once

#include "public.h"

#include <yt/yt/server/lib/lsm/compaction_hints.h>
#include <yt/yt/server/lib/lsm/lsm_backend.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECompactionHintState, ui8,
    // Terminal state.
    ((DefinitelyNoHint)   (0))

    ((NotInEpoch)         (1))
    ((DisabledByConfig)   (2))
    // Store has BadState if state is not EStoreState::Persistent.
    // Partition has BadState if it contains a store which is not persistent or definitely
    // has no relevant compaction hint payload.
    ((BadState)           (3))

    // When controller state set in active it goes though the following stages.
    //  1) Fetch pipelines to get payload.
    //  2) After pipeline finished, controller waits for LSM feedback.
    //  3) After feedback is received, controller will just store LSM compaction hint.
    ((Active)             (4))
);

class TCompactionHintConfigChange
{
public:
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(OldEnable);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(NewEnable);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(ConfigChanged);

public:
    TCompactionHintConfigChange(
        const TTableMountConfigPtr& oldConfig,
        const TTableMountConfigPtr& newConfig,
        NLsm::EStoreCompactionHintKind kind);

    TCompactionHintConfigChange(
        const TTableMountConfigPtr& oldConfig,
        const TTableMountConfigPtr& newConfig,
        NLsm::EPartitionCompactionHintKind kind);

    TCompactionHintConfigChange AsOnlyEnableConfigChange();
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipelinePtr BuildFetchPipeline(
    TSortedChunkStore* store,
    NLsm::EStoreCompactionHintKind kind);

bool DefinitelyHasNoHint(
    TSortedChunkStore* store,
    NLsm::EStoreCompactionHintKind kind);

bool DefinitelyHasNoHint(
    TPartition* partition,
    NLsm::EPartitionCompactionHintKind kind);

////////////////////////////////////////////////////////////////////////////////

//! Holds strong pointers to fetch pipelines of all compaction hint payload kinds.
//! Exists per-store and most of time is empty.
class TCompactionHintFetchPipelines
{
    // There is definitely no hint if optional is empty.
    using TFetchPipelines = NLsm::TStoreCompactionHintArray<std::optional<TWeakPtr<TCompactionHintFetchPipeline>>>;

    using TStoreFetchPipelines = NLsm::TCalculatableStoreCompactionHintArray<TCompactionHintFetchPipelinePtr>;
    using TPartitionFetchPipelines = NLsm::TPartitionCompactionHintArray<TCompactionHintFetchPipelinePtr>;

public:
    // Provides centralized access to fetch pipeline of compaction hint kind
    // for store and partition controllers.
    DEFINE_BYREF_RO_PROPERTY(TFetchPipelines, FetchPipelines, {
        {NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow, nullptr},
        {NLsm::EStoreCompactionHintKind::VersionedRowDigest, nullptr},
        {NLsm::EStoreCompactionHintKind::MinHashDigest, nullptr},
    });

public:
    bool DefinitelyHasNoHint(NLsm::EStoreCompactionHintKind kind) const;

    void OnStoreHasNoHint(NLsm::EStoreCompactionHintKind kind);

    void ResetStorePipeline(NLsm::EStoreCompactionHintKind kind);
    void ResetPartitionPipeline(NLsm::EPartitionCompactionHintKind kind);

    void InitializeStorePipeline(TSortedChunkStore* store, NLsm::EStoreCompactionHintKind kind);
    void InitializePartitionPipeline(TSortedChunkStore* store, NLsm::EPartitionCompactionHintKind kind);

private:
    TCompactionHintFetchPipelinePtr GetOrCreateFetchPipeline(
        TSortedChunkStore* store,
        NLsm::EStoreCompactionHintKind kind);

    TStoreFetchPipelines StoreFetchPipelines_;
    TPartitionFetchPipelines PartitionFetchPipelines_;
};

////////////////////////////////////////////////////////////////////////////////

//! Base class for storing compaction hints.
/*
 * Exists permanently within either a store or a partition. Coordinates hint fetch pipelines.
 * When the pipeline is finished, keeps a reference to the pipeline until recevied LSM feedback and then holds
 * the computed LSM compaction hint until revision is changed.
 */
template <class TDerived, class TLsmCompactionHint, class TOwner>
class TCompactionHintControllerBase
{
public:
    DEFINE_BYREF_RO_PROPERTY(TLsmCompactionHint, LsmCompactionHint);
    DEFINE_BYVAL_RO_PROPERTY(ECompactionHintState, State, ECompactionHintState::NotInEpoch);

    DEFINE_BYVAL_RO_PROPERTY(NLsm::EStoreCompactionHintKind, StoreCompactionHintKind);
    DEFINE_BYVAL_RO_PROPERTY(NLsm::EPartitionCompactionHintKind, PartitionCompactionHintKind);

public:
    TCompactionHintControllerBase() = default;

    TCompactionHintControllerBase(
        NLsm::EStoreCompactionHintKind storeKind,
        NLsm::EPartitionCompactionHintKind partitionKind = NLsm::EPartitionCompactionHintKind::None);

    void OnLsmFeedbackReceived(TOwner* owner, TLsmCompactionHint&& lsmCompactionHint);

protected:
    bool FetchInProgress() const;

    void SetPassiveState(TOwner* owner, ECompactionHintState state);
    void SetActiveState(TOwner* owner);

    void SetDeterminedState(TOwner* owner, TCompactionHintConfigChange configChange, bool isInBadState);

    void UpdateRevision();
};

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactionHintController
    : public TCompactionHintControllerBase<TStoreCompactionHintController, NLsm::TStoreCompactionHint, TSortedChunkStore>
{
public:
    using TBase = TCompactionHintControllerBase<TStoreCompactionHintController, NLsm::TStoreCompactionHint, TSortedChunkStore>;

public:
    using TBase::TBase;

    void StartEpoch(TSortedChunkStore* store);
    void StopEpoch(TSortedChunkStore* store);

    void OnMountConfigUpdated(TSortedChunkStore* store, const TTableMountConfigPtr& oldConfig);

    void OnStoreStateChanged(TSortedChunkStore* store);

    void OnStoreHasNoHint(TSortedChunkStore* store);

private:
    friend TBase;

    void ResetPipelines(TSortedChunkStore* store);
    void InitializePipelines(TSortedChunkStore* store);
};

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactionHints
{
    using TControllers = NLsm::TCalculatableStoreCompactionHintArray<TStoreCompactionHintController>;

public:
    DEFINE_BYREF_RW_PROPERTY(TControllers, Controllers, {
        {NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow, {NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow}},
        {NLsm::EStoreCompactionHintKind::VersionedRowDigest, {NLsm::EStoreCompactionHintKind::VersionedRowDigest}},
    });

public:
    void Initialize(TSortedChunkStore* store);

    void StartEpoch(TSortedChunkStore* store);
    void StopEpoch(TSortedChunkStore* store);

    void OnMountConfigUpdated(TSortedChunkStore* store, const TTableMountConfigPtr& oldConfig);

    void OnStoreStateChanged(TSortedChunkStore* store);

    void OnLsmFeedbackReceived(
        TSortedChunkStore* store,
        NLsm::TCompactionHintUpdateRequest::TStoreRequest&& storeRequest);

private:
    template <class TFunction>
    void ForEachController(TFunction&& function, ECompactionHintState minState);
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionCompactionHintController
    : public TCompactionHintControllerBase<TPartitionCompactionHintController, NLsm::TPartitionCompactionHint, TPartition>
{
public:
    using TBase = TCompactionHintControllerBase<TPartitionCompactionHintController, NLsm::TPartitionCompactionHint, TPartition>;

public:
    using TBase::TBase;

    void StartEpoch(TPartition* partition);
    void StopEpoch(TPartition* partition);

    void OnMountConfigUpdated(TPartition* partition, const TTableMountConfigPtr& oldConfig);

    void OnStoreStateChanged(TPartition* partition, TSortedChunkStore* store);

    void OnStoreHasNoHint(TPartition* partition, TSortedChunkStore* store);
    void OnPartitionHasNoHint(TPartition* partition);

    void OnStoreRemoved(TPartition* partition, TSortedChunkStore* store);
    void OnStoreAdded(TPartition* partition, TSortedChunkStore* store);

private:
    friend TBase;
    friend void Serialize(
        const TPartitionCompactionHintController& partitionCompactionHintController,
        NYson::IYsonConsumer* consumer);

    bool AreAllStoresGood() const;
    bool DefinitelyHasNoHint(TSortedChunkStore* store) const;

    void RecalculateCounts(TPartition* partition);

    void ResetPipelines(TPartition* partition);
    void InitializePipelines(TPartition* partition);

    // Stores definitely without relevant hint payload.
    int NoHintStoreCount_ = 0;
    // Stores which state is not Persistent.
    int BadStateStoreCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionCompactionHints
{
    using TControllers = NLsm::TPartitionCompactionHintArray<TPartitionCompactionHintController>;
public:
    DEFINE_BYREF_RW_PROPERTY(TControllers, Controllers, {{
        NLsm::EPartitionCompactionHintKind::MinHashDigest, {
            NLsm::EStoreCompactionHintKind::MinHashDigest,
            NLsm::EPartitionCompactionHintKind::MinHashDigest,
        }}
    });

public:
    void Initialize(TPartition* partition);

    void StopEpoch(TPartition* partition);
    void StartEpoch(TPartition* partition);

    void OnMountConfigUpdated(TPartition* partition, const TTableMountConfigPtr& oldConfig);

    void OnStoreStateChanged(TPartition* partition, TSortedChunkStore* store);

    void OnStoreHasNoHint(TPartition* partition, TSortedChunkStore* store, NLsm::EStoreCompactionHintKind kind);

    void OnStoreRemoved(TPartition* partition, TSortedChunkStore* store);
    void OnStoreAdded(TPartition* partition, TSortedChunkStore* store);

    void OnLsmFeedbackReceived(
        TPartition* partition,
        NLsm::TCompactionHintUpdateRequest::TPartitionRequest&& partitionRequest);

private:
    template <class TPartitionFunction, class TStoreFunction>
    void ForEachController(
        TPartitionFunction&& partitionFunction,
        TStoreFunction&& storeFunction,
        TPartition* partition,
        ECompactionHintState minState,
        bool onlyLeadingState = true);
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TCompactionHintFetchPipelines& CompactionHintFetchPipelines,
    NYson::IYsonConsumer* consumer);

void Serialize(
    const TStoreCompactionHints& storeCompactionHints,
    NYson::IYsonConsumer* consumer);

void Serialize(
    const TPartitionCompactionHints& partitionCompactionHints,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
