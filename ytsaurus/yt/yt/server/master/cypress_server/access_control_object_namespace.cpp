#include "access_control_object_namespace.h"
#include "access_control_object.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NCypressServer {

///////////////////////////////////////////////////////////////////////////////

TAccessControlObjectNamespace::TAccessControlObjectNamespace(
    TAccessControlObjectNamespaceId id)
    : TObject(id)
    , Acd_(this)
{ }

TAccessControlObject* TAccessControlObjectNamespace::FindMember(const TString& memberName) const
{
    auto it = Members_.find(memberName);
    return it == Members_.end() ? nullptr : it->second;
}

void TAccessControlObjectNamespace::RegisterMember(TAccessControlObject* member)
{
    YT_VERIFY(Members_.emplace(member->GetName(), member).second);
}

void TAccessControlObjectNamespace::UnregisterMember(TAccessControlObject* member)
{
    YT_VERIFY(Members_.erase(member->GetName()) == 1);
}

void TAccessControlObjectNamespace::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    TObject::Save(context);

    Save(context, Name_);
    Save(context, Acd_);
    // NB: Not saving Members_.
}

void TAccessControlObjectNamespace::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    TObject::Load(context);

    Load(context, Name_);
    Load(context, Acd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
