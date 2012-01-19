#include "stdafx.h"
#include "object_detail.h"

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TObjectBase::TObjectBase()
    : RefCounter(0)
{ }

TObjectBase::TObjectBase(const TObjectBase& other)
    : RefCounter(other.RefCounter)
{ }

i32 TObjectBase::RefObject()
{
    return ++RefCounter;
}

i32 TObjectBase::UnrefObject()
{
    YASSERT(RefCounter > 0);
    return --RefCounter;
}

i32 TObjectBase::GetObjectRefCounter() const
{
    return RefCounter;
}

////////////////////////////////////////////////////////////////////////////////

TObjectWithIdBase::TObjectWithIdBase()
{ }

TObjectWithIdBase::TObjectWithIdBase(const TObjectId& id)
    : Id_(id)
{ }

TObjectWithIdBase::TObjectWithIdBase(const TObjectWithIdBase& other)
    : TObjectBase(other)
    , Id_(other.Id_)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

