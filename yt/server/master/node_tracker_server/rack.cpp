#include "rack.h"
#include "data_center.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TRack::TRack(TRackId id)
    : TObject(id)
    , Index_(-1)
{ }

TString TRack::GetLowercaseObjectName() const
{
    return Format("rack %Qv", GetName());
}

TString TRack::GetCapitalizedObjectName() const
{
    return Format("Rack %Qv", GetName());
}

void TRack::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Index_);
    Save(context, DataCenter_);
}

void TRack::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Index_);
    Load(context, DataCenter_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

