#include "user.h"

#include <yt/server/security_server/security_manager.pb.h>

#include <yt/server/cell_master/serialize.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYson;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void TUserStatistics::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, RequestCount);
    if (context.GetVersion() >= 200) {
        Persist(context, ReadRequestTime);
        Persist(context, WriteRequestTime);
    }
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

TUser::TUser(const TUserId& id)
    : TSubject(id)
    , Banned_(false)
    , RequestQueueSizeLimit_(100)
    , RequestQueueSize_(0)
    , LocalStatisticsPtr_(nullptr)
    , RequestStatisticsUpdateIndex_(-1)
    , ReadRequestRateLimit_(100)
    , WriteRequestRateLimit_(100)
{ }

void TUser::Save(NCellMaster::TSaveContext& context) const
{
    TSubject::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, ReadRequestRateLimit_);
    Save(context, WriteRequestRateLimit_);
    Save(context, RequestQueueSizeLimit_);
    Save(context, MulticellStatistics_);
    Save(context, ClusterStatistics_);
}

void TUser::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    // COMPAT(aozeritsky)
    if (context.GetVersion() < 815) {
        auto requestRateLimit = Load<int>(context);
        ReadRequestRateLimit_ = requestRateLimit;
        WriteRequestRateLimit_ = requestRateLimit;
    } else {
        Load(context, ReadRequestRateLimit_);
        Load(context, WriteRequestRateLimit_);
    }
    Load(context, RequestQueueSizeLimit_);
    Load(context, MulticellStatistics_);
    Load(context, ClusterStatistics_);
}

TUserStatistics& TUser::CellStatistics(NObjectClient::TCellTag cellTag)
{
    auto it = MulticellStatistics_.find(cellTag);
    YCHECK(it != MulticellStatistics_.end());
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

const NConcurrency::IReconfigurableThroughputThrottlerPtr TUser::GetRequestRateThrottler(EUserWorkloadType workloadType)
{
    switch (workloadType) {
        case EUserWorkloadType::Read:
            return ReadRequestRateThrottler_;
        case EUserWorkloadType::Write:
            return WriteRequestRateThrottler_;
        default:
            Y_UNREACHABLE();
    }
}

void TUser::SetRequestRateThrottler(
    const NConcurrency::IReconfigurableThroughputThrottlerPtr& throttler,
    EUserWorkloadType workloadType)
{
    switch (workloadType) {
        case EUserWorkloadType::Read:
            ReadRequestRateThrottler_ = throttler;
            break;
        case EUserWorkloadType::Write:
            WriteRequestRateThrottler_ = throttler;
            break;
        default:
            Y_UNREACHABLE();
    }
}

int TUser::GetRequestRateLimit(EUserWorkloadType type)
{
    switch (type) {
        case EUserWorkloadType::Read:
            return ReadRequestRateLimit_;
        case EUserWorkloadType::Write:
            return WriteRequestRateLimit_;
        default:
            Y_UNREACHABLE();
    }
}

void TUser::SetRequestRateLimit(int limit, EUserWorkloadType type)
{
    switch (type) {
        case EUserWorkloadType::Read:
            ReadRequestRateLimit_ = limit;
            break;
        case EUserWorkloadType::Write:
            WriteRequestRateLimit_ = limit;
            break;
        default:
            Y_UNREACHABLE();
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

