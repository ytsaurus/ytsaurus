#include "store_detail.h"
#include "private.h"
#include "automaton.h"
#include "tablet.h"
#include "config.h"

#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTabletNode {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TStoreBase::TStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : Config_(std::move(config))
    , StoreId_(id)
    , Tablet_(tablet)
    , PerformanceCounters_(Tablet_->GetPerformanceCounters())
    , TabletId_(Tablet_->GetId())
    , Schema_(Tablet_->Schema())
    , KeyColumns_(Tablet_->KeyColumns())
    , KeyColumnCount_(Tablet_->GetKeyColumnCount())
    , SchemaColumnCount_(Tablet_->GetSchemaColumnCount())
    , ColumnLockCount_(Tablet_->GetColumnLockCount())
    , LockIndexToName_(Tablet_->LockIndexToName())
    , ColumnIndexToLockIndex_(Tablet_->ColumnIndexToLockIndex())
{
    Logger = TabletNodeLogger;
    Logger.AddTag("StoreId: %v", StoreId_);
}

TStoreBase::~TStoreBase()
{
    i64 delta = -MemoryUsage_;
    MemoryUsage_ = 0;
    MemoryUsageUpdated_.Fire(delta);
}

TStoreId TStoreBase::GetId() const
{
    return StoreId_;
}

TTablet* TStoreBase::GetTablet() const
{
    return Tablet_;
}

EStoreState TStoreBase::GetStoreState() const
{
    return StoreState_;
}

void TStoreBase::SetStoreState(EStoreState state)
{
    StoreState_ = state;
}

i64 TStoreBase::GetMemoryUsage() const
{
    return MemoryUsage_;
}

void TStoreBase::SubscribeMemoryUsageUpdated(const TCallback<void(i64 delta)>& callback)
{
    MemoryUsageUpdated_.Subscribe(callback);
    callback.Run(+GetMemoryUsage());
}

void TStoreBase::UnsubscribeMemoryUsageUpdated(const TCallback<void(i64 delta)>& callback)
{
    MemoryUsageUpdated_.Unsubscribe(callback);
    callback.Run(-GetMemoryUsage());
}

void TStoreBase::SetMemoryUsage(i64 value)
{
    if (std::abs(value - MemoryUsage_) > MemoryUsageGranularity) {
        i64 delta = value - MemoryUsage_;
        MemoryUsage_ = value;
        MemoryUsageUpdated_.Fire(delta);
    }
}

TOwningKey TStoreBase::RowToKey(TUnversionedRow row)
{
    return NTabletNode::RowToKey(Schema_, KeyColumns_, row);
}

TOwningKey TStoreBase::RowToKey(TSortedDynamicRow row)
{
    return NTabletNode::RowToKey(Schema_, KeyColumns_, row);
}

void TStoreBase::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, StoreState_);
}

void TStoreBase::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, StoreState_);
}

void TStoreBase::BuildOrchidYson(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("store_state").Value(StoreState_);
}

bool TStoreBase::IsDynamic() const
{
    return false;
}

IDynamicStorePtr TStoreBase::AsDynamic()
{
    YUNREACHABLE();
}

bool TStoreBase::IsChunk() const
{
    return false;
}

IChunkStorePtr TStoreBase::AsChunk()
{
    YUNREACHABLE();
}

bool TStoreBase::IsSorted() const
{
    return false;
}

ISortedStorePtr TStoreBase::AsSorted()
{
    YUNREACHABLE();
}

TSortedDynamicStorePtr TStoreBase::AsSortedDynamic()
{
    YUNREACHABLE();
}

TSortedChunkStorePtr TStoreBase::AsSortedChunk()
{
    YUNREACHABLE();
}

bool TStoreBase::IsOrdered() const
{
    return false;
}

IOrderedStorePtr TStoreBase::AsOrdered()
{
    YUNREACHABLE();
}

TOrderedDynamicStorePtr TStoreBase::AsOrderedDynamic()
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

TDynamicStoreBase::TDynamicStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
    , Atomicity_(Tablet_->GetAtomicity())
    , RowBuffer_(New<TRowBuffer>(
        Config_->PoolChunkSize,
        Config_->MaxPoolSmallBlockRatio))
{
    StoreState_ = EStoreState::ActiveDynamic;
}

i64 TDynamicStoreBase::GetLockCount() const
{
    return StoreLockCount_;
}

i64 TDynamicStoreBase::Lock()
{
    YASSERT(Atomicity_ == EAtomicity::Full);

    auto result = ++StoreLockCount_;
    LOG_TRACE("Store locked (Count: %v)",
        result);
    return result;
}

i64 TDynamicStoreBase::Unlock()
{
    YASSERT(Atomicity_ == EAtomicity::Full);
    YASSERT(StoreLockCount_ > 0);

    auto result = --StoreLockCount_;
    LOG_TRACE("Store unlocked (Count: %v)",
        result);
    return result;
}

void TDynamicStoreBase::SetStoreState(EStoreState state)
{
    if (StoreState_ == EStoreState::ActiveDynamic && state == EStoreState::PassiveDynamic) {
        OnSetPassive();
    }
    TStoreBase::SetStoreState(state);
}

i64 TDynamicStoreBase::GetUncompressedDataSize() const
{
    return GetPoolCapacity();
}

EStoreFlushState TDynamicStoreBase::GetFlushState() const
{
    return FlushState_;
}

void TDynamicStoreBase::SetFlushState(EStoreFlushState state)
{
    FlushState_ = state;
}

i64 TDynamicStoreBase::GetValueCount() const
{
    return StoreValueCount_;
}

i64 TDynamicStoreBase::GetPoolSize() const
{
    return RowBuffer_->GetSize();
}

i64 TDynamicStoreBase::GetPoolCapacity() const
{
    return RowBuffer_->GetCapacity();
}

void TDynamicStoreBase::BuildOrchidYson(IYsonConsumer* consumer)
{
    TStoreBase::BuildOrchidYson(consumer);

    BuildYsonMapFluently(consumer)
        .Item("flush_state").Value(FlushState_)
        .Item("row_count").Value(GetRowCount())
        .Item("lock_count").Value(GetLockCount())
        .Item("value_count").Value(GetValueCount())
        .Item("pool_size").Value(GetPoolSize())
        .Item("pool_capacity").Value(GetPoolCapacity());
}

bool TDynamicStoreBase::IsDynamic() const
{
    return true;
}

IDynamicStorePtr TDynamicStoreBase::AsDynamic()
{
    return this;
}

///////////////////////////////////////////////////////////////////////////////

TChunkStoreBase::TChunkStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
{ }

EStorePreloadState TChunkStoreBase::GetPreloadState() const
{
    return PreloadState_;
}

void TChunkStoreBase::SetPreloadState(EStorePreloadState state)
{
    PreloadState_ = state;
}

TFuture<void> TChunkStoreBase::GetPreloadFuture() const
{
    return PreloadFuture_;
}

void TChunkStoreBase::SetPreloadFuture(TFuture<void> future)
{
    PreloadFuture_ = future;
}

EStoreCompactionState TChunkStoreBase::GetCompactionState() const
{
    return CompactionState_;
}

void TChunkStoreBase::SetCompactionState(EStoreCompactionState state)
{
    CompactionState_ = state;
}

bool TChunkStoreBase::IsChunk() const
{
    return true;
}

IChunkStorePtr TChunkStoreBase::AsChunk()
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

TSortedStoreBase::TSortedStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
{ }

TPartition* TSortedStoreBase::GetPartition() const
{
    return Partition_;
}

void TSortedStoreBase::SetPartition(TPartition* partition)
{
    Partition_ = partition;
}

bool TSortedStoreBase::IsSorted() const
{
    return true;
}

ISortedStorePtr TSortedStoreBase::AsSorted()
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

TOrderedStoreBase::TOrderedStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
{ }

bool TOrderedStoreBase::IsOrdered() const
{
    return true;
}

IOrderedStorePtr TOrderedStoreBase::AsOrdered()
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

