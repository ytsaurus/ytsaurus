#include "user.h"
#include "yt/yt/server/master/security_server/private.h"

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TUserRequestLimitsOptions::TUserRequestLimitsOptions()
{
    RegisterParameter("default", Default)
        .GreaterThan(0)
        .Default(100);
    RegisterParameter("per_cell", PerCell)
        .Optional();

    RegisterPostprocessor([&] () {
        for (const auto& [cellTag, value] : PerCell) {
            if (cellTag < NObjectClient::MinValidCellTag || cellTag > NObjectClient::MaxValidCellTag) {
                THROW_ERROR_EXCEPTION("Invalid cell tag %v",
                    cellTag);
            }

            if (value <= 0) {
                THROW_ERROR_EXCEPTION("Invalid limit for cell %v: value %v must be greater than zero",
                    cellTag,
                    value);
            }
        }
    });
}

void TUserRequestLimitsOptions::SetValue(NObjectServer::TCellTag cellTag, std::optional<int> value)
{
    if (cellTag == NObjectClient::InvalidCellTag) {
        Default = value;
    } else {
        YT_VERIFY(value);
        PerCell[cellTag] = *value;
    }
}

std::optional<int> TUserRequestLimitsOptions::GetValue(NObjectServer::TCellTag cellTag) const
{
    if (auto it = PerCell.find(cellTag)) {
        return it->second;
    }
    return Default;
}

////////////////////////////////////////////////////////////////////////////////

TUserQueueSizeLimitsOptions::TUserQueueSizeLimitsOptions()
{
    RegisterParameter("default", Default)
        .GreaterThan(0)
        .Default(100);
    RegisterParameter("per_cell", PerCell)
        .Optional();

    RegisterPostprocessor([&] () {
        for (const auto& [cellTag, value] : PerCell) {
            if (cellTag < NObjectClient::MinValidCellTag || cellTag > NObjectClient::MaxValidCellTag) {
                THROW_ERROR_EXCEPTION("Invalid cell tag %v",
                    cellTag);
            }

            if (value <= 0) {
                THROW_ERROR_EXCEPTION("Invalid limit for cell %v: value %v must be greater than zero",
                    cellTag,
                    value);
            }
        }
    });
}

void TUserQueueSizeLimitsOptions::SetValue(NObjectServer::TCellTag cellTag, int value)
{
    if (cellTag == NObjectClient::InvalidCellTag) {
        Default = value;
    } else {
        PerCell[cellTag] = value;
    }
}

int TUserQueueSizeLimitsOptions::GetValue(NObjectServer::TCellTag cellTag) const
{
    return PerCell.Value(cellTag, Default);
}

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

    auto profiler = SecurityProfiler
        .WithSparse()
        .WithTag("user", Name_);

    ReadTimeCounter_ = profiler.TimeCounter("/user_read_time");
    WriteTimeCounter_ = profiler.TimeCounter("/user_write_time");
    ReadRequestCounter_ = profiler.Counter("/user_read_request_count");
    WriteRequestCounter_ = profiler.Counter("/user_write_request_count");
    RequestCounter_ = profiler.Counter("/user_request_count");
    RequestQueueSizeSummary_ = profiler.Summary("/user_request_queue_size");
}

int TUser::GetRequestQueueSize() const
{
    return RequestQueueSize_;
}

void TUser::SetRequestQueueSize(int size)
{
    RequestQueueSize_ = size;
    RequestQueueSizeSummary_.Record(size);
}

void TUser::UpdateCounters(const TUserWorkload& workload)
{
    RequestCounter_.Increment(workload.RequestCount);
    switch (workload.Type) {
        case EUserWorkloadType::Read:
            ReadRequestCounter_.Increment(workload.RequestCount);
            ReadTimeCounter_.Add(workload.RequestTime);
            break;
        case EUserWorkloadType::Write:
            WriteRequestCounter_.Increment(workload.RequestCount);
            WriteTimeCounter_.Add(workload.RequestTime);
            break;
        default:
            YT_ABORT();
    }
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

std::optional<int> TUser::GetRequestRateLimit(EUserWorkloadType type, NObjectServer::TCellTag cellTag) const
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

void TUser::SetRequestRateLimit(std::optional<int> limit, EUserWorkloadType type, NObjectServer::TCellTag cellTag)
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

