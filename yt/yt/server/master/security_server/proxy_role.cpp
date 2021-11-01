#include "proxy_role.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TProxyRole::TProxyRole(TProxyRoleId id)
    : TObject(id)
    , Acd_(this)
{ }

TString TProxyRole::GetLowercaseObjectName() const
{
    return Format("%Qlv proxy role %Qv", ProxyKind_, Name_);
}

TString TProxyRole::GetCapitalizedObjectName() const
{
    return Format("%Qv proxy role %Qv", ProxyKind_, Name_);
}

void TProxyRole::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, ProxyKind_);
    Save(context, Acd_);
}

void TProxyRole::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, ProxyKind_);
    Load(context, Acd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
