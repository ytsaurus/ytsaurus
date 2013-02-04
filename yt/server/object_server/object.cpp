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

int TObjectBase::GetObjectRefCounter() const
{
    return RefCounter;
}

bool TObjectBase::IsAlive() const
{
    return RefCounter > 0;
}

bool TObjectBase::IsTrunk() const
{
    if (!IsTypeVersioned(TypeFromId(Id))) {
        return true;
    }

    auto* node = static_cast<const TCypressNodeBase*>(this);
    return node->GetTrunkNode() == node;
}

void TObjectBase::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    ::Save(output, RefCounter);
}

void TObjectBase::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    ::Load(input, RefCounter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
