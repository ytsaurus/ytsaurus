#include "network_project.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TNetworkProject::TNetworkProject(TNetworkProjectId id)
    : TObject(id)
    , Acd_(this)
{ }

TString TNetworkProject::GetLowercaseObjectName() const
{
    return Format("network project %Qv", Name_);
}

TString TNetworkProject::GetCapitalizedObjectName() const
{
    return Format("Network project %Qv", Name_);
}

TString TNetworkProject::GetObjectPath() const
{
    return Format("//sys/network_projects/%v", GetName());
}

void TNetworkProject::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, ProjectId_);
    Save(context, Acd_);
}

void TNetworkProject::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, ProjectId_);
    Load(context, Acd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
