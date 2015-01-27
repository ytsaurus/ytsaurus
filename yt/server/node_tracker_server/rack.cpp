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
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Index_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

