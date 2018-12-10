#include "data_center.h"

#include <yt/server/cell_master/serialize.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TDataCenter::TDataCenter(const TDataCenterId& id)
    : TObjectBase(id)
{ }

void TDataCenter::Save(NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
}

void TDataCenter::Load(NCellMaster::TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
