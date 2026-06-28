#include "object_detail.h"

namespace NYT::NTabletNode {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TObjectId TObjectBase::GetId() const
{
    return Id_;
}

TObjectBase::TObjectBase(TObjectId id)
    : Id_(id)
{ }

// Out-of-line so this TU is the single vtable emission point (key function).
TObjectBase::~TObjectBase() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
