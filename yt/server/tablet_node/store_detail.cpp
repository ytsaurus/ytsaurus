#include "stdafx.h"
#include "store_detail.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TStoreBase::TStoreBase(
    const TStoreId& id,
    TTablet* tablet)
    : Id_(id)
    , Tablet_(tablet)
    , Partition_(nullptr)
{ }

TStoreId TStoreBase::GetId() const
{
    return Id_;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

