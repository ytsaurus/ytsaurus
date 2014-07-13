#include "stdafx.h"
#include "user.h"

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TUser::TUser(const TUserId& id)
    : TSubject(id)
    , Banned_(false)
    , RequestRateLimit_(100.0)
    , RequestCounter_(0)
    , AccessTime_(TInstant::Zero())
    , RequestStatisticsUpdate_(nullptr)
{
    ResetRequestRate();
}

void TUser::Save(NCellMaster::TSaveContext& context) const
{
    TSubject::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, RequestRateLimit_);
    Save(context, RequestCounter_);
    Save(context, AccessTime_);
}

void TUser::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    Load(context, RequestRateLimit_);
    Load(context, RequestCounter_);
    Load(context, AccessTime_);
}

void TUser::ResetRequestRate()
{
    CheckpointTime_ = TInstant::Zero();
    CheckpointRequestCounter_ = 0;
    RequestRate_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

