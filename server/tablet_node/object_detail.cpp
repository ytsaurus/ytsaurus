#include "object_detail.h"

namespace NYT {
namespace NTabletNode {

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

} // namespace NTabletNode
} // namespace NYT
