#include "network_project.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NSecurityServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TNetworkProject::TNetworkProject(TNetworkProjectId id)
    : TObject(id)
    , Acd_(this)
{ }

std::string TNetworkProject::GetLowercaseObjectName() const
{
    return Format("network project %Qv", Name_);
}

std::string TNetworkProject::GetCapitalizedObjectName() const
{
    return Format("Network project %Qv", Name_);
}

TYPath TNetworkProject::GetObjectPath() const
{
    return Format("//sys/network_projects/%v", NYPath::ToYPathLiteral(GetName()));
}

void TNetworkProject::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, ProjectId_);
    PHOENIX_REGISTER_FIELD(3, Acd_);
}

PHOENIX_DEFINE_TYPE(TNetworkProject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
