#include "proxy_role.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TProxyRole::TProxyRole(TProxyRoleId id)
    : TObject(id)
    , Acd_(this)
{ }

std::string TProxyRole::GetLowercaseObjectName() const
{
    return Format("%Qlv proxy role %Qv", ProxyKind_, Name_);
}

std::string TProxyRole::GetCapitalizedObjectName() const
{
    return Format("%Qv proxy role %Qv", ProxyKind_, Name_);
}

void TProxyRole::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, ProxyKind_);
    PHOENIX_REGISTER_FIELD(3, Acd_);
}

PHOENIX_DEFINE_TYPE(TProxyRole);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
