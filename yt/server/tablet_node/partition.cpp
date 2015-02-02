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

TPartition::TPartition(TTablet* tablet, const TPartitionId& id, int index)
    : Tablet_(tablet)
    , Id_(id)
    , Index_(index)
    , PivotKey_(MinKey())
    , NextPivotKey_(MaxKey())
    , State_(EPartitionState::Normal)
    , SampleKeys_(New<TKeyList>())
{ }

TPartition::~TPartition()
{ }

void TPartition::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, PivotKey_);
    Save(context, NextPivotKey_);
    Save(context, SamplingTime_);
    Save(context, SamplingRequestTime_);
    Save(context, *SampleKeys_);

    Save(context, Stores_.size());
    for (auto store : Stores_) {
        Save(context, store->GetId());
    }
}

void TPartition::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, PivotKey_);
    Load(context, NextPivotKey_);
    Load(context, SamplingTime_);
    Load(context, SamplingRequestTime_);
    Load(context, *SampleKeys_);

    size_t storeCount = Load<size_t>(context);
    for (size_t index = 0; index < storeCount; ++index) {
        auto storeId = Load<TStoreId>(context);
        auto store = Tablet_->GetStore(storeId);
        YCHECK(Stores_.insert(store).second);
    }
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

TPartitionSnapshotPtr TPartition::BuildSnapshot() const
{
    auto snapshot = New<TPartitionSnapshot>();
    snapshot->Id = Id_;
    snapshot->PivotKey = PivotKey_;
    snapshot->SampleKeys = SampleKeys_;
    snapshot->Stores.insert(snapshot->Stores.end(), Stores_.begin(), Stores_.end());
    return snapshot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

