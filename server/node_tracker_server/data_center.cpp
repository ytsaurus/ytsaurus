#include "data_center.h"

#include <yt/server/cell_master/serialize.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TDataCenter::TDataCenter(const TDataCenterId& id)
    : IObjectBase(id)
{ }

void TDataCenter::Save(NCellMaster::TSaveContext& context) const
{
    IObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
}

void TDataCenter::Load(NCellMaster::TLoadContext& context)
{
    IObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
