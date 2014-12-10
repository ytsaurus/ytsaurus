#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TKeyList
    : public TIntrinsicRefCounted
{
    std::vector<TOwningKey> Keys;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

};

DEFINE_REFCOUNTED_TYPE(TKeyList)

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSnapshot
    : public TIntrinsicRefCounted
{
    TOwningKey PivotKey;
    TKeyListPtr SampleKeys;
    std::vector<IStorePtr> Stores;
};

DEFINE_REFCOUNTED_TYPE(TPartitionSnapshot)

////////////////////////////////////////////////////////////////////////////////

class TPartition
    : private TNonCopyable
{
public:
    static const int EdenIndex = -1;

    DEFINE_BYVAL_RO_PROPERTY(TTablet*, Tablet);
    DEFINE_BYVAL_RW_PROPERTY(int, Index);

    DEFINE_BYVAL_RW_PROPERTY(TOwningKey, PivotKey);
    DEFINE_BYVAL_RW_PROPERTY(TOwningKey, NextPivotKey);

    DEFINE_BYREF_RW_PROPERTY(yhash_set<IStorePtr>, Stores);

    DEFINE_BYVAL_RW_PROPERTY(EPartitionState, State);

    DEFINE_BYVAL_RW_PROPERTY(bool, SamplingNeeded);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastSamplingTime);
    DEFINE_BYVAL_RW_PROPERTY(TKeyListPtr, SampleKeys);

public:
    TPartition(TTablet* tablet, int index);
    ~TPartition();

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    i64 GetUncompressedDataSize() const;
    i64 GetUnmergedRowCount() const;

    TPartitionSnapshotPtr BuildSnapshot() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
