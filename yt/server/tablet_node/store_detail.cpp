#include "stdafx.h"
#include "store_detail.h"
#include "tablet.h"

namespace NYT {
namespace NTabletNode {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TStoreBase::TStoreBase(
    const TStoreId& id,
    TTablet* tablet)
    : StoreId_(id)
    , Tablet_(tablet)
    , Statistics_(Tablet_->GetStatistics())
    , TabletId_(Tablet_->GetId())
    , Schema_(Tablet_->Schema())
    , KeyColumns_(Tablet_->KeyColumns())
    , KeyColumnCount_(Tablet_->GetKeyColumnCount())
    , SchemaColumnCount_(Tablet_->GetSchemaColumnCount())
    , ColumnLockCount_(Tablet_->GetColumnLockCount())
    , LockIndexToName_(Tablet_->LockIndexToName())
    , ColumnIndexToLockIndex_(Tablet_->ColumnIndexToLockIndex())
{ }

TStoreId TStoreBase::GetId() const
{
    return StoreId_;
}

TTablet* TStoreBase::GetTablet() const
{
    return Tablet_;
}

EStoreState TStoreBase::GetState() const
{
    return State_;
}

void TStoreBase::SetState(EStoreState state)
{
    State_ = state;
}

TPartition* TStoreBase::GetPartition() const
{
    return Partition_;
}

void TStoreBase::SetPartition(TPartition* partition)
{
    Partition_ = partition;
}

void TStoreBase::Save(TSaveContext& /*context*/) const
{ }

void TStoreBase::Load(TLoadContext& /*context*/)
{ }

void TStoreBase::BuildOrchidYson(IYsonConsumer* /*consumer*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

