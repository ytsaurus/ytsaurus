#pragma once

#include "acl.h"
#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TProxyRole
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);
    DEFINE_BYVAL_RW_PROPERTY(NApi::EProxyKind, ProxyKind);
    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

public:
    using TObject::TObject;
    explicit TProxyRole(TProxyRoleId id);

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
