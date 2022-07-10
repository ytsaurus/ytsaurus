#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TAccessControlObject
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYREF_RW_PROPERTY(TAccessControlObjectNamespacePtr, Namespace);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, PrincipalAcd);

    explicit TAccessControlObject(TAccessControlObjectId id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TAccessControlObject)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
