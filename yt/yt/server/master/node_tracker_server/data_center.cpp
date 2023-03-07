#include "data_center.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TDataCenter::TDataCenter(TDataCenterId id)
    : TObject(id)
{ }

TString TDataCenter::GetLowercaseObjectName() const
{
    return Format("data center %Qv", GetName());
}

TString TDataCenter::GetCapitalizedObjectName() const
{
    return Format("Data center %Qv", GetName());
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
