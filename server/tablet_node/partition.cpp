#include "partition.h"
#include "automaton.h"
#include "store.h"
#include "tablet.h"
#include "config.h"

#include <yt/ytlib/table_client/serialize.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/utilex/random.h>

namespace NYT {
namespace NTabletNode {

using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

void TSampleKeyList::Save(TSaveContext& context) const
{
    using NYT::Save;
    TWireProtocolWriter writer;
    writer.WriteUnversionedRowset(Keys);
    Save(context, MergeRefsToRef<TSampleKeyListTag>(writer.Finish()));
}

void TSampleKeyList::Load(TLoadContext& context)
{
    using NYT::Load;
    TWireProtocolReader reader(
        Load<TSharedRef>(context),
        New<TRowBuffer>(TSampleKeyListTag()));
    Keys = reader.ReadUnversionedRowset(true);
}

////////////////////////////////////////////////////////////////////////////////

TPartition::TPartition(
    TTablet* tablet,
    const TPartitionId& id,
    int index,
    TOwningKey pivotKey,
    TOwningKey nextPivotKey)
    : TObjectBase(id)
    , Tablet_(tablet)
    , Index_(index)
    , PivotKey_(std::move(pivotKey))
    , NextPivotKey_(std::move(nextPivotKey))
    , SampleKeys_(New<TSampleKeyList>())
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
            auto store = Tablet_->GetStore(storeId)->AsSorted();
            YCHECK(Stores_.insert(store).second);
        }
    }
}

TCallback<void(TSaveContext&)> TPartition::AsyncSave()
{
    return BIND([snapshot = BuildSnapshot()] (TSaveContext& context) {
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

i64 TPartition::GetCompressedDataSize() const
{
    i64 result = 0;
    for (const auto& store : Stores_) {
        result += store->GetCompressedDataSize();
    }
    return result;
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

TPartitionSnapshotPtr TPartition::BuildSnapshot() const
{
    auto snapshot = New<TPartitionSnapshot>();
    snapshot->Id = Id_;
    snapshot->PivotKey = PivotKey_;
    snapshot->NextPivotKey = NextPivotKey_;
    snapshot->SampleKeys = SampleKeys_;
    snapshot->Stores.insert(snapshot->Stores.end(), Stores_.begin(), Stores_.end());
    return snapshot;
}

void TPartition::StartEpoch()
{
    CompactionTime_ = TInstant::Now();

    const auto& config = Tablet_->GetConfig();
    if (config->AutoCompactionPeriod) {
        CompactionTime_ -= RandomDuration(*config->AutoCompactionPeriod);
    }
}

void TPartition::StopEpoch()
{
    State_ = EPartitionState::Normal;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionIdFormatter::operator()(TStringBuilder* builder, const std::unique_ptr<TPartition>& partition) const
{
    FormatValue(builder, partition->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

