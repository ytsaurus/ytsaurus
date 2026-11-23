#include "data_center.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NNodeTrackerServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string TDataCenter::GetLowercaseObjectName() const
{
    return Format("data center %Qv", GetName());
}

std::string TDataCenter::GetCapitalizedObjectName() const
{
    return Format("Data center %Qv", GetName());
}

TYPath TDataCenter::GetObjectPath() const
{
    return Format("//sys/data_centers/%v", NYPath::ToYPathLiteral(GetName()));
}

void TDataCenter::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
}

PHOENIX_DEFINE_TYPE(TDataCenter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
