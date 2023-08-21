#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

namespace NYT::NCypressServer {

///////////////////////////////////////////////////////////////////////////////

class TAccessControlObjectNamespace
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);
    using TMembers = THashMap<TString, TAccessControlObject*>;
    DEFINE_BYREF_RW_PROPERTY(TMembers, Members);

public:
    explicit TAccessControlObjectNamespace(TAccessControlObjectNamespaceId id);

    TAccessControlObject* FindMember(const TString& memberName) const;

    void RegisterMember(TAccessControlObject* member);
    void UnregisterMember(TAccessControlObject* member);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TAccessControlObjectNamespace)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
