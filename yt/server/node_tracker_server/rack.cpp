#include "rack.h"
#include "data_center.h"

#include <yt/server/cell_master/serialize.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TRack::TRack(const TRackId& id)
    : TObjectBase(id)
    , Index_(-1)
{ }

void TRack::Save(NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Index_);
    Save(context, DataCenter_);
}

void TRack::Load(NCellMaster::TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Index_);
    // COMPAT(shakurov)
    if (context.GetVersion() >= 400) {
        Load(context, DataCenter_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

