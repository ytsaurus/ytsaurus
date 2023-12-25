#include "partition.h"
#include "automaton.h"
#include "store.h"
#include "tablet.h"
#include "structured_logger.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/table_client/serialize.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NTabletNode {

using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

void TSampleKeyList::Save(TSaveContext& context) const
{
    using NYT::Save;
    auto writer = CreateWireProtocolWriter();
    writer->WriteUnversionedRowset(Keys);
    Save(context, MergeRefsToRef<TSampleKeyListTag>(writer->Finish()));
}

void TSampleKeyList::Load(TLoadContext& context)
{
    using NYT::Load;
    auto reader = CreateWireProtocolReader(
        Load<TSharedRef>(context),
        New<TRowBuffer>(TSampleKeyListTag()));
    Keys = reader->ReadUnversionedRowset(true);
}

////////////////////////////////////////////////////////////////////////////////

TPartition::TPartition(
    TTablet* tablet,
    TPartitionId id,
    int index,
    TLegacyOwningKey pivotKey,
    TLegacyOwningKey nextPivotKey)
    : TObjectBase(id)
    , Tablet_(tablet)
    , Index_(index)
    , PivotKey_(std::move(pivotKey))
    , NextPivotKey_(std::move(nextPivotKey))
    , SampleKeys_(New<TSampleKeyList>())
{ }

void TPartition::SetState(EPartitionState state)
{
    State_ = state;
    Tablet_->GetStructuredLogger()->OnPartitionStateChanged(this);
}

EPartitionState TPartition::GetState() const
{
    return State_;
}

void TPartition::CheckedSetState(EPartitionState oldState, EPartitionState newState)
{
    YT_VERIFY(GetState() == oldState);
    SetState(newState);
}

void TPartition::Save(TSaveContext& context) const
{
    using NYT::Save;

    // COMPAT(babenko): drop these
    Save(context, TInstant::Zero());
    Save(context, TInstant::Zero());

    TSizeSerializer::Save(context, Stores_.size());

    auto storeIterators = GetSortedIterators(
        Stores_,
        [] (const auto& lhs, const auto& rhs) { return lhs->GetId() < rhs->GetId(); });
    for (auto it : storeIterators) {
        Save(context, (*it)->GetId());
    }
}

void TPartition::Load(TLoadContext& context)
{
    using NYT::Load;

    // COMPAT(babenko): drop these
    Load(context, SamplingTime_);
    Load(context, SamplingRequestTime_);

    int storeCount = TSizeSerializer::LoadSuspended(context);
    SERIALIZATION_DUMP_WRITE(context, "stores[%v]", storeCount);
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < storeCount; ++index) {
            SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
            SERIALIZATION_DUMP_INDENT(context) {
                auto storeId = Load<TStoreId>(context);
                auto store = Tablet_->GetStore(storeId)->AsSorted();
                InsertOrCrash(Stores_, store);
            }
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

    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    if (mountConfig->AutoCompactionPeriod) {
        CompactionTime_ -= RandomDuration(*mountConfig->AutoCompactionPeriod);
    }

    AllowedSplitTime_ = TInstant::Zero();
}

void TPartition::StopEpoch()
{
    State_ = EPartitionState::Normal;
}

void TPartition::RequestImmediateSplit(std::vector<TLegacyOwningKey> pivotKeys)
{
    PivotKeysForImmediateSplit_ = std::move(pivotKeys);
}

bool TPartition::IsImmediateSplitRequested() const
{
    return !PivotKeysForImmediateSplit_.empty();
}

void TPartition::ResetRowDigestRequestTime()
{
    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    if (auto period = mountConfig->RowDigestCompaction->CheckPeriod) {
        RowDigestRequestTime_ = TInstant::Now() - RandomDuration(*period);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionIdFormatter::operator()(TStringBuilderBase* builder, const std::unique_ptr<TPartition>& partition) const
{
    FormatValue(builder, partition->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

