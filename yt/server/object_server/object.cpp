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
    : Id_(id)
{ }

TObjectBase::~TObjectBase()
{
    YASSERT(RefCounter_ == 0);
    // To make debugging easier.
    RefCounter_ = -1;
}

const TObjectId& TObjectBase::GetId() const
{
    return Id_;
}

EObjectType TObjectBase::GetType() const
{
    return TypeFromId(Id_);
}

bool TObjectBase::IsBuiltin() const
{
    return IsWellKnownId(Id_);
}

int TObjectBase::RefObject()
{
    YASSERT(RefCounter_ >= 0);
    return ++RefCounter_;
}

int TObjectBase::UnrefObject()
{
    YASSERT(RefCounter_ > 0);
    return --RefCounter_;
}

int TObjectBase::WeakRefObject()
{
    YCHECK(IsAlive());
    YASSERT(WeakRefCounter_ >= 0);
    return ++WeakRefCounter_;
}

int TObjectBase::WeakUnrefObject()
{
    YASSERT(WeakRefCounter_ > 0);
    return --WeakRefCounter_;
}

void TObjectBase::ResetWeakRefCounter()
{
    WeakRefCounter_ = 0;
}

int TObjectBase::GetObjectRefCounter() const
{
    return RefCounter_;
}

int TObjectBase::GetObjectWeakRefCounter() const
{
    return WeakRefCounter_;
}

bool TObjectBase::IsAlive() const
{
    return RefCounter_ > 0;
}

bool TObjectBase::IsLocked() const
{
    return WeakRefCounter_ > 0;
}

bool TObjectBase::IsTrunk() const
{
    if (!IsVersionedType(TypeFromId(Id_))) {
        return true;
    }

    auto* node = static_cast<const TCypressNodeBase*>(this);
    return node->GetTrunkNode() == node;
}

void TObjectBase::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, RefCounter_);
}

void TObjectBase::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RefCounter_);
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
