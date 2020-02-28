#pragma once

#include "acl.h"
#include "public.h"

#include <yt/server/master/object_server/object.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TNetworkProject
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

    DEFINE_BYVAL_RW_PROPERTY(ui32, ProjectId);

    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

public:
    explicit TNetworkProject(TNetworkProjectId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
