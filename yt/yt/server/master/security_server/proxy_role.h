#pragma once

#include "acl.h"
#include "public.h"

#include <yt/server/master/object_server/object.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TProxyRole
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

    DEFINE_BYVAL_RW_PROPERTY(NSecurityClient::EProxyKind, ProxyKind);

    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

public:
    explicit TProxyRole(TProxyRoleId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
