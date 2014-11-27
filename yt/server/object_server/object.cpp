#include "stdafx.h"
#include "object.h"

#include <ytlib/object_client/helpers.h>

#include <server/cypress_server/node.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NObjectServer {

using namespace NObjectClient;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

TObjectBase::TObjectBase(const TObjectId& id)
    : Id(id)
{ }

const TObjectId& TObjectBase::GetId() const
{
    return Id;
}

EObjectType TObjectBase::GetType() const
{
    return TypeFromId(Id);
}

bool TObjectBase::IsBuiltin() const
{
    return IsWellKnownId(Id);
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

int TObjectBase::WeakRefObject()
{
    YCHECK(IsAlive());
    YASSERT(WeakRefCounter >= 0);
    return ++WeakRefCounter;
}

int TObjectBase::WeakUnrefObject()
{
    YASSERT(WeakRefCounter > 0);
    return --WeakRefCounter;
}

void TObjectBase::ResetWeakRefCounter()
{
    WeakRefCounter = 0;
}

int TObjectBase::GetObjectRefCounter() const
{
    return RefCounter;
}

int TObjectBase::GetObjectWeakRefCounter() const
{
    return WeakRefCounter;
}

bool TObjectBase::IsAlive() const
{
    return RefCounter > 0;
}

bool TObjectBase::IsLocked() const
{
    return WeakRefCounter > 0;
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

////////////////////////////////////////////////////////////////////////////////

TNonversionedObjectBase::TNonversionedObjectBase(const TObjectId& id)
    : TObjectBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
