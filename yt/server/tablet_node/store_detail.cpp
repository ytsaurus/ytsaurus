#include "stdafx.h"
#include "store_detail.h"
#include "tablet.h"
#include "automaton.h"
#include "private.h"

#include <core/ytree/fluent.h>

namespace NYT {
namespace NTabletNode {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const i64 MemoryUsageGranularity = (i64) 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TStoreBase::TStoreBase(
    const TStoreId& id,
    TTablet* tablet)
    : StoreId_(id)
    , Tablet_(tablet)
    , PerformanceCounters_(Tablet_->GetPerformanceCounters())
    , TabletId_(Tablet_->GetTabletId())
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

TPartition* TStoreBase::GetPartition() const
{
    return Partition_;
}

void TStoreBase::SetPartition(TPartition* partition)
{
    Partition_ = partition;
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

TOwningKey TStoreBase::RowToKey(TDynamicRow row)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

