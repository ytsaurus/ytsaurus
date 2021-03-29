#include "subject.h"
#include "group.h"
#include "user.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSubject::TSubject(TSubjectId id)
    : TNonversionedObjectBase(id)
    , Acd_(this)
{ }

void TSubject::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, MemberOf_);
    Save(context, RecursiveMemberOf_);
    Save(context, LinkedObjects_);
    Save(context, Acd_);
    Save(context, Aliases_);
}

void TSubject::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, MemberOf_);
    Load(context, RecursiveMemberOf_);
    Load(context, LinkedObjects_);
    Load(context, Acd_);
    Load(context, Aliases_);
}

bool TSubject::IsUser() const
{
    return GetType() == EObjectType::User;
}

TUser* TSubject::AsUser()
{
    YT_VERIFY(IsUser());
    return static_cast<TUser*>(this);
}

bool TSubject::IsGroup() const
{
    return GetType() == EObjectType::Group;
}

TGroup* TSubject::AsGroup()
{
    YT_VERIFY(IsGroup());
    return static_cast<TGroup*>(this);
}

void TSubject::LinkObject(TObject* object)
{
    YT_ASSERT(object->IsTrunk());
    auto it = LinkedObjects_.find(object);
    if (it == LinkedObjects_.end()) {
        YT_VERIFY(LinkedObjects_.emplace(object, 1).second);
    } else {
        ++it->second;
    }
}

void TSubject::UnlinkObject(TObject* object)
{
    YT_ASSERT(object->IsTrunk());
    auto it = LinkedObjects_.find(object);
    YT_VERIFY(it != LinkedObjects_.end());
    if (--it->second == 0) {
        LinkedObjects_.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

