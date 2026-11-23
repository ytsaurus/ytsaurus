#include "subject.h"
#include "group.h"
#include "user.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSubject::TSubject(TSubjectId id)
    : TObject(id)
    , Acd_(this)
{ }

const std::string& TSubject::GetName() const
{
    return Name_;
}

void TSubject::SetName(const std::string& name)
{
    Name_ = name;
}

void TSubject::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, MemberOf_);
    PHOENIX_REGISTER_FIELD(3, RecursiveMemberOf_);
    PHOENIX_REGISTER_FIELD(4, LinkedObjects_);
    PHOENIX_REGISTER_FIELD(5, Acd_);
    PHOENIX_REGISTER_FIELD(6, Aliases_);
}

PHOENIX_DEFINE_TYPE(TSubject);

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

