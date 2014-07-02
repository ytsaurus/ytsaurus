#include "stdafx.h"
#include "subject.h"
#include "user.h"
#include "group.h"

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NSecurityServer {

using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSubject::TSubject(const TAccountId& id)
    : TNonversionedObjectBase(id)
{ }

void TSubject::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, MemberOf_);
    Save(context, RecursiveMemberOf_);
    Save(context, LinkedObjects_);
}

void TSubject::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, MemberOf_);
    Load(context, RecursiveMemberOf_);
    Load(context, LinkedObjects_);
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

void TSubject::LinkObject(TObjectBase* object)
{
    auto it = LinkedObjects_.find(object);
    if (it == LinkedObjects_.end()) {
        YCHECK(LinkedObjects_.insert(std::make_pair(object, 1)).second);
    } else {
        ++it->second;
    }
}

void TSubject::UnlinkObject(TObjectBase* object)
{
    auto it = LinkedObjects_.find(object);
    YCHECK(it != LinkedObjects_.end());
    if (--it->second == 0) {
        LinkedObjects_.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

