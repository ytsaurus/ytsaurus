#include "subject.h"
#include "group.h"
#include "user.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSubject::TSubject(TSubjectId id)
    : TObject(id)
    , Acd_(this)
{ }

TString TSubject::GetName() const
{
    return Name_;
}

void TSubject::SetName(const TString& name)
{
    Name_ = name;
}

void TSubject::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

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
    TObject::Load(context);

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
        EmplaceOrCrash(LinkedObjects_, object, 1);
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

