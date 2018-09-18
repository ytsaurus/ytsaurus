#pragma once

#include "public.h"
#include "object_detail.h"

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TSampleKeyListTag
{ };

struct TSampleKeyList
    : public TIntrinsicRefCounted
{
    TSharedRange<TKey> Keys;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);
};

DEFINE_REFCOUNTED_TYPE(TSampleKeyList)

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSnapshot
    : public TIntrinsicRefCounted
{
    TPartitionId Id;
    TOwningKey PivotKey;
    TOwningKey NextPivotKey;
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

    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, NextPivotKey);

    DEFINE_BYREF_RW_PROPERTY(THashSet<ISortedStorePtr>, Stores);

    DEFINE_BYVAL_RW_PROPERTY(EPartitionState, State, EPartitionState::Normal);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, SamplingTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, SamplingRequestTime);
    DEFINE_BYVAL_RW_PROPERTY(TSampleKeyListPtr, SampleKeys);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CompactionTime);

public:
    TPartition(
        TTablet* tablet,
        const TPartitionId& id,
        int index,
        TOwningKey pivotKey = TOwningKey(),
        TOwningKey nextPivotKey = TOwningKey());

    void CheckedSetState(EPartitionState oldState, EPartitionState newState);

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

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionIdFormatter
{
public:
    void operator()(TStringBuilder* builder, const std::unique_ptr<TPartition>& partition) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
