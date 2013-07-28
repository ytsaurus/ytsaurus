#include "stdafx.h"
#include "object.h"

#include <server/cypress_server/node.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NObjectServer {

using namespace NObjectClient;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

TObjectBase::TObjectBase(const TObjectId& id)
    : Id(id)
    , RefCounter(0)
    , LockCounter(0)
{ }

const TObjectId& TObjectBase::GetId() const
{
    return Id;
}

EObjectType TObjectBase::GetType() const
{
    return TypeFromId(Id);
}

int TObjectBase::RefObject()
{
    YASSERT(RefCounter >= 0);
    return ++RefCounter;
}

int TObjectBase::UnrefObject()
{
    YASSERT(RefCounter > 0);
    return --RefCounter;
}

int TObjectBase::LockObject()
{
    YCHECK(IsAlive());
    YASSERT(LockCounter >= 0);
    return ++LockCounter;
}

int TObjectBase::UnlockObject()
{
    YASSERT(LockCounter > 0);
    return --LockCounter;
}

void TObjectBase::ResetObjectLocks()
{
    LockCounter = 0;
}

int TObjectBase::GetObjectRefCounter() const
{
    return RefCounter;
}

int TObjectBase::GetObjectLockCounter() const
{
    return LockCounter;
}

bool TObjectBase::IsAlive() const
{
    return RefCounter > 0;
}

bool TObjectBase::IsLocked() const
{
    return LockCounter > 0;
}

bool TObjectBase::IsTrunk() const
{
    if (!IsVersionedType(TypeFromId(Id))) {
        return true;
    }

    auto* node = static_cast<const TCypressNodeBase*>(this);
    return node->GetTrunkNode() == node;
}

void TObjectBase::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, RefCounter);
}

void TObjectBase::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RefCounter);
}

TObjectId GetObjectId(const TObjectBase* object)
{
    return object ? object->GetId() : NullObjectId;
}

bool IsObjectAlive(const TObjectBase* object)
{
    return object && object->IsAlive();
}

bool CompareObjectsForSerialization(const TObjectBase* lhs, const TObjectBase* rhs)
{
    return GetObjectId(lhs) < GetObjectId(rhs);
}

////////////////////////////////////////////////////////////////////////////////

TNonversionedObjectBase::TNonversionedObjectBase(const TObjectId& id)
    : TObjectBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
