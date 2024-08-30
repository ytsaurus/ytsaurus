#include "data_center.h"

#include <yt/yt/server/master/cell_master/serialize.h>

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
    return Format("//sys/data_centers/%v", GetName());
}

void TDataCenter::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
}

void TDataCenter::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
