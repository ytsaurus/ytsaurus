#include "network_project.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TNetworkProject::TNetworkProject(TNetworkProjectId id)
    : TNonversionedObjectBase(id)
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

void TNetworkProject::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, ProjectId_);
    Save(context, Acd_);
}

void TNetworkProject::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, ProjectId_);

    // COMPAT(gritukan)
    using NCellMaster::EMasterReign;
    if (context.GetVersion() >= EMasterReign::FixNetworkProjectSerialization ||
       (context.GetVersion() >= EMasterReign::FixNetworkProjectSerialization_19_8 && context.GetVersion() < EMasterReign::TruncateJournals))
    {
        Load(context, Acd_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
