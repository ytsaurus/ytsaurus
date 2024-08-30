#pragma once

#include "acl.h"
#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TNetworkProject
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);
    DEFINE_BYVAL_RW_PROPERTY(ui32, ProjectId);
    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

public:
    using TObject::TObject;
    explicit TNetworkProject(TNetworkProjectId id);

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
