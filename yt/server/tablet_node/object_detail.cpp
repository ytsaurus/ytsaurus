#include "object_detail.h"

namespace NYT::NTabletNode {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

const TObjectId& TObjectBase::GetId() const
{
    return Id_;
}

TObjectBase::TObjectBase(const TObjectId& id)
    : Id_(id)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
