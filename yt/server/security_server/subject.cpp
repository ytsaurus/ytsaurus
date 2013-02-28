#include "stdafx.h"
#include "subject.h"
#include "user.h"
#include "group.h"

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NSecurityServer {

using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSubject::TSubject(const TAccountId& id)
    : TNonversionedObjectBase(id)
{ }

void TSubject::Save(const NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    auto* output = context.GetOutput();
    ::Save(output, Name_);
    SaveObjectRefs(context, MemberOf_);
    SaveObjectRefs(context, RecursiveMemberOf_);
    SaveObjectRefs(context, ReferencingObjects_);
}

void TSubject::Load(const NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    auto* input = context.GetInput();
    ::Load(input, Name_);
    LoadObjectRefs(context, MemberOf_);
    LoadObjectRefs(context, RecursiveMemberOf_);
    LoadObjectRefs(context, ReferencingObjects_);
}

TUser* TSubject::AsUser()
{
    YCHECK(GetType() == EObjectType::User);
    return static_cast<TUser*>(this);
}

TGroup* TSubject::AsGroup()
{
    YCHECK(GetType() == EObjectType::Group);
    return static_cast<TGroup*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

