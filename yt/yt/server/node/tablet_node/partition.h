#pragma once

#include "public.h"
#include "object_detail.h"
#include "compaction_hint_controllers.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TSampleKeyListTag
{ };

struct TSampleKeyList
    : public TRefCounted
{
    TSharedRange<TLegacyKey> Keys;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context, const IMemoryUsageTrackerPtr& tracker);
};

DEFINE_REFCOUNTED_TYPE(TSampleKeyList)

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSnapshot
    : public TRefCounted
{
    TPartitionId Id;
    TLegacyOwningKey PivotKey;
    TLegacyOwningKey NextPivotKey;
    TSampleKeyListPtr SampleKeys;
    std::vector<ISortedStorePtr> Stores;
};

DEFINE_REFCOUNTED_TYPE(TPartitionSnapshot)

////////////////////////////////////////////////////////////////////////////////

class TPartition
    : public TObjectBase
    , public TRefTracked<TPartition>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTablet*, Tablet);
    DEFINE_BYVAL_RW_PROPERTY(int, Index);

    DEFINE_BYVAL_RO_PROPERTY(TLegacyOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(TLegacyOwningKey, NextPivotKey);

    DEFINE_BYREF_RO_PROPERTY(THashSet<ISortedStorePtr>, Stores);

    // NB: These are transient.
    DECLARE_BYVAL_RW_PROPERTY(EPartitionState, State);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, SamplingTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, SamplingRequestTime);

    DEFINE_BYVAL_RW_PROPERTY(TSampleKeyListPtr, SampleKeys);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CompactionTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AllowedSplitTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AllowedMergeTime);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TLegacyOwningKey>, PivotKeysForImmediateSplit);

    DEFINE_BYREF_RW_PROPERTY(TPartitionCompactionHints, CompactionHints);

public:
    TPartition(
        TTablet* tablet,
        TPartitionId id,
        int index,
        TLegacyOwningKey pivotKey = TLegacyOwningKey(),
        TLegacyOwningKey nextPivotKey = TLegacyOwningKey());

    ~TPartition();

    void CheckedSetState(EPartitionState oldState, EPartitionState newState);

    void EnterCompactionState(EPartitionState stateToAdd);
    void ExitCompactionState(EPartitionState stateToRemove);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TCallback<void(TSaveContext&)> AsyncSave();
    void AsyncLoad(TLoadContext& context);

    i64 GetCompressedDataSize() const;
    i64 GetUncompressedDataSize() const;
    i64 GetUnmergedRowCount() const;

    bool IsEden() const;

    TPartitionSnapshotPtr BuildSnapshot() const;

    void StartEpoch();
    void StopEpoch();

    void RequestImmediateSplit(std::vector<TLegacyOwningKey> pivotKeys);
    bool IsImmediateSplitRequested() const;

    void BuildOrchidYson(NYTree::TFluentMap fluent) const;

    void AddStore(const ISortedStorePtr& store);
    void RemoveStore(const ISortedStorePtr& store);

private:
    EPartitionState State_ = EPartitionState::Normal;

    static constexpr ui32 CompactingMask = 0b001;
    static constexpr ui32 PartitioningMask = 0b010;
    static constexpr ui32 CompactionNotAllowedMask = 0b100;

    static ui32 StateToMask(EPartitionState state);
    static EPartitionState MaskToState(ui32 mask);
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionIdFormatter
{
public:
    void operator()(TStringBuilderBase* builder, const std::unique_ptr<TPartition>& partition) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
