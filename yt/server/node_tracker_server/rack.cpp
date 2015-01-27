#include "stdafx.h"
#include "rack.h"

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TRack::TRack(const TRackId& id)
    : TObjectBase(id)
    , Index_(-1)
{ }

TRackSet TRack::GetIndexMask() const
{
    YASSERT(Index_ > NullRackIndex && Index_ < MaxRackCount);
    return 1ULL << Index_;
}

void TRack::Save(NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Index_);
}

void TRack::Load(NCellMaster::TLoadContext& context)
{
    // COMPAT(babenko)
    if (context.GetVersion() >= 105) {
        TObjectBase::Load(context);
    } else {
        RefCounter_ = 1;
    }

    using NYT::Load;
    Load(context, Name_);
    Load(context, Index_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

