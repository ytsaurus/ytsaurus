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

void TUserStatistics::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, RequestCount);
    Persist(context, ReadRequestTime);
    Persist(context, WriteRequestTime);
    Persist(context, AccessTime);
}

void ToProto(NProto::TUserStatistics* protoStatistics, const TUserStatistics& statistics)
{
    protoStatistics->set_request_count(statistics.RequestCount);
    protoStatistics->set_read_request_time(ToProto<i64>(statistics.ReadRequestTime));
    protoStatistics->set_write_request_time(ToProto<i64>(statistics.WriteRequestTime));
    protoStatistics->set_access_time(ToProto<i64>(statistics.AccessTime));
}

void FromProto(TUserStatistics* statistics, const NProto::TUserStatistics& protoStatistics)
{
    statistics->RequestCount = protoStatistics.request_count();
    statistics->ReadRequestTime = FromProto<TDuration>(protoStatistics.read_request_time());
    statistics->WriteRequestTime = FromProto<TDuration>(protoStatistics.write_request_time());
    statistics->AccessTime = FromProto<TInstant>(protoStatistics.access_time());
}

void Serialize(const TUserStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("request_count").Value(statistics.RequestCount)
            .Item("read_request_time").Value(statistics.ReadRequestTime)
            .Item("write_request_time").Value(statistics.WriteRequestTime)
            .Item("access_time").Value(statistics.AccessTime)
        .EndMap();
}

TUserStatistics& operator += (TUserStatistics& lhs, const TUserStatistics& rhs)
{
    lhs.RequestCount += rhs.RequestCount;
    lhs.ReadRequestTime += rhs.ReadRequestTime;
    lhs.WriteRequestTime += rhs.WriteRequestTime;
    lhs.AccessTime = std::max(lhs.AccessTime, rhs.AccessTime);
    return lhs;
}

TUserStatistics operator + (const TUserStatistics& lhs, const TUserStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TUser::TUser(TUserId id)
    : TSubject(id)
    , RequestLimits_(New<TUserRequestLimitsConfig>())
{ }

TString TUser::GetObjectName() const
{
    return Format("User %Qv", Name_);
}

void TUser::Save(NCellMaster::TSaveContext& context) const
{
    TSubject::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, *RequestLimits_);
    Save(context, MulticellStatistics_);
    Save(context, ClusterStatistics_);
}

void TUser::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);

    using NYT::Load;
    Load(context, Banned_);

    int readRequestRateLimit = 100;
    int writeRequestRateLimit = 100;
    int requestQueueSizeLimit = 100;

    // COMPAT(aozeritsky)
    if (context.GetVersion() < NCellMaster::EMasterReign::AddReadRequestRateLimitAndWriteRequestRateLimit) {
        auto requestRateLimit = Load<int>(context);
        readRequestRateLimit = requestRateLimit;
        writeRequestRateLimit = requestRateLimit;
    } else if (context.GetVersion() < NCellMaster::EMasterReign::RequestLimits) {
        Load(context, readRequestRateLimit);
        Load(context, writeRequestRateLimit);
    }

    if (context.GetVersion() < NCellMaster::EMasterReign::RequestLimits) {
        Load(context, requestQueueSizeLimit);
    }

    if (context.GetVersion() < NCellMaster::EMasterReign::RequestLimits) {
        RequestLimits_->ReadRequestRateLimits->Default = readRequestRateLimit;
        RequestLimits_->WriteRequestRateLimits->Default = writeRequestRateLimit;
        RequestLimits_->RequestQueueSizeLimits->Default = requestQueueSizeLimit;
    } else {
        Load(context, *RequestLimits_);
    }

    Load(context, MulticellStatistics_);
    Load(context, ClusterStatistics_);
}

TUserStatistics& TUser::CellStatistics(NObjectClient::TCellTag cellTag)
{
    auto it = MulticellStatistics_.find(cellTag);
    YT_VERIFY(it != MulticellStatistics_.end());
    return it->second;
}

TUserStatistics& TUser::LocalStatistics()
{
    return *LocalStatisticsPtr_;
}

void TUser::RecomputeClusterStatistics()
{
    ClusterStatistics_ = TUserStatistics();
    for (const auto& pair : MulticellStatistics_) {
        ClusterStatistics_ += pair.second;
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

