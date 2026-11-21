#include "rack.h"
#include "data_center.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NNodeTrackerServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string TRack::GetLowercaseObjectName() const
{
    return Format("rack %Qv", GetName());
}

std::string TRack::GetCapitalizedObjectName() const
{
    return Format("Rack %Qv", GetName());
}

TYPath TRack::GetObjectPath() const
{
    return Format("//sys/racks/%v", NYPath::ToYPathLiteral(GetName()));
}

void TRack::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, Index_);
    PHOENIX_REGISTER_FIELD(3, DataCenter_);
}

PHOENIX_DEFINE_TYPE(TRack);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

