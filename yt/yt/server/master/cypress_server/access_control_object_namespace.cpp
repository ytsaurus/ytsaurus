#include "access_control_object_namespace.h"
#include "access_control_object.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

TAccessControlObjectNamespace::TAccessControlObjectNamespace(
    TAccessControlObjectNamespaceId id)
    : TObject(id)
    , Acd_(this)
{ }

TAccessControlObject* TAccessControlObjectNamespace::FindMember(const std::string& memberName) const
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

void TAccessControlObjectNamespace::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, Acd_);
    // NB: Not saving Members_.
}

PHOENIX_DEFINE_TYPE(TAccessControlObjectNamespace);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
