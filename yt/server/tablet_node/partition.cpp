#include "stdafx.h"
#include "partition.h"
#include "automaton.h"
#include "store.h"
#include "tablet.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

void TKeyList::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Keys);
}

void TKeyList::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, Keys);
}

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_
const int TPartition::EdenIndex;
#endif

TPartition::TPartition(
    TTablet* tablet,
    const TPartitionId& id,
    int index,
    TOwningKey pivotKey,
    TOwningKey nextPivotKey)
    : Tablet_(tablet)
    , Id_(id)
    , Index_(index)
    , PivotKey_(std::move(pivotKey))
    , NextPivotKey_(std::move(nextPivotKey))
    , State_(EPartitionState::Normal)
    , SampleKeys_(New<TKeyList>())
{ }

void TPartition::CheckedSetState(EPartitionState oldState, EPartitionState newState)
{
    YCHECK(GetState() == oldState);
    SetState(newState);
}

void TPartition::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, SamplingTime_);
    Save(context, SamplingRequestTime_);

    TSizeSerializer::Save(context, Stores_.size());
    // NB: This is not stable.
    for (const auto& store : Stores_) {
        Save(context, store->GetId());
    }
}

void TPartition::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, SamplingTime_);
    Load(context, SamplingRequestTime_);

    int storeCount = TSizeSerializer::LoadSuspended(context);
    SERIALIZATION_DUMP_WRITE(context, "stores[%v]", storeCount);
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < storeCount; ++index) {
            auto storeId = Load<TStoreId>(context);
            auto store = Tablet_->GetStore(storeId);
            YCHECK(Stores_.insert(store).second);
        }
    }
}

TCallback<void(TSaveContext&)> TPartition::AsyncSave()
{
    return BIND([snapshot = Snapshot_] (TSaveContext& context) {
        using NYT::Save;

        Save(context, snapshot->PivotKey);
        Save(context, snapshot->NextPivotKey);
        Save(context, *snapshot->SampleKeys);
    });
}

void TPartition::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    Load(context, PivotKey_);
    Load(context, NextPivotKey_);
    Load(context, *SampleKeys_);
}

i64 TPartition::GetUncompressedDataSize() const
{
    i64 result = 0;
    for (const auto& store : Stores_) {
        result += store->GetUncompressedDataSize();
    }
    return result;
}

i64 TPartition::GetUnmergedRowCount() const
{
    i64 result = 0;
    for (const auto& store : Stores_) {
        result += store->GetRowCount();
    }
    return result;
}

bool TPartition::IsEden() const
{
    return Index_ == EdenIndex;
}

TPartitionSnapshotPtr TPartition::RebuildSnapshot()
{
    Snapshot_ = New<TPartitionSnapshot>();
    Snapshot_->Id = Id_;
    Snapshot_->PivotKey = PivotKey_;
    Snapshot_->NextPivotKey = NextPivotKey_;
    Snapshot_->SampleKeys = SampleKeys_;
    Snapshot_->Stores.insert(Snapshot_->Stores.end(), Stores_.begin(), Stores_.end());
    return Snapshot_;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionIdFormatter::operator()(
    TStringBuilder* builder,
    const std::unique_ptr<TPartition>& partition) const
{
    FormatValue(builder, partition->GetId(), STRINGBUF("v"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

