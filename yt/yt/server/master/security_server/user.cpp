#include "user.h"

#include <yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/server/master/cell_master/serialize.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TUser::TUser(TUserId id)
    : TSubject(id)
    , RequestLimits_(New<TUserRequestLimitsConfig>())
{ }

TString TUser::GetLowercaseObjectName() const
{
    return Format("user %Qv", Name_);
}

TString TUser::GetCapitalizedObjectName() const
{
    return Format("User %Qv", Name_);
}

void TUser::Save(NCellMaster::TSaveContext& context) const
{
    TSubject::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, *RequestLimits_);
}

void TUser::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    Load(context, *RequestLimits_);
}

const NConcurrency::IReconfigurableThroughputThrottlerPtr& TUser::GetRequestRateThrottler(EUserWorkloadType workloadType)
{
    switch (workloadType) {
        case EUserWorkloadType::Read:
            return ReadRequestRateThrottler_;
        case EUserWorkloadType::Write:
            return WriteRequestRateThrottler_;
        default:
            YT_ABORT();
    }
}

void TUser::SetRequestRateThrottler(
    NConcurrency::IReconfigurableThroughputThrottlerPtr throttler,
    EUserWorkloadType workloadType)
{
    switch (workloadType) {
        case EUserWorkloadType::Read:
            ReadRequestRateThrottler_ = std::move(throttler);
            break;
        case EUserWorkloadType::Write:
            WriteRequestRateThrottler_ = std::move(throttler);
            break;
        default:
            YT_ABORT();
    }
}

int TUser::GetRequestRateLimit(EUserWorkloadType type, NObjectServer::TCellTag cellTag) const
{
    switch (type) {
        case EUserWorkloadType::Read:
            return RequestLimits_->ReadRequestRateLimits->GetValue(cellTag);
        case EUserWorkloadType::Write:
            return RequestLimits_->WriteRequestRateLimits->GetValue(cellTag);
        default:
            YT_ABORT();
    }
}

void TUser::SetRequestRateLimit(int limit, EUserWorkloadType type, NObjectServer::TCellTag cellTag)
{
    switch (type) {
        case EUserWorkloadType::Read:
            RequestLimits_->ReadRequestRateLimits->SetValue(cellTag, limit);
            break;
        case EUserWorkloadType::Write:
            RequestLimits_->WriteRequestRateLimits->SetValue(cellTag, limit);
            break;
        default:
            YT_ABORT();
    }
}

int TUser::GetRequestQueueSizeLimit(NObjectServer::TCellTag cellTag) const
{
    return RequestLimits_->RequestQueueSizeLimits->GetValue(cellTag);
}

void TUser::SetRequestQueueSizeLimit(int limit, NObjectServer::TCellTag cellTag)
{
    RequestLimits_->RequestQueueSizeLimits->SetValue(cellTag, limit);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

