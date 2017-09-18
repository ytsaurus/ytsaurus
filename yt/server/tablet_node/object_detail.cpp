#include "object_detail.h"

namespace NYT {
namespace NTabletNode {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

const TObjectId& IObjectBase::GetId() const
{
    return Id_;
}

IObjectBase::IObjectBase(const TObjectId& id)
    : Id_(id)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
