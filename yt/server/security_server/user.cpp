#include "stdafx.h"
#include "user.h"

#include <core/ytree/fluent.h>

#include <server/security_server/security_manager.pb.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TUserStatistics::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, RequestCounter);
    if (context.IsSave() || context.LoadContext().GetVersion() >= 200) {
        Persist(context, ReadRequestTimer);
        Persist(context, WriteRequestTimer);
    }
    Persist(context, AccessTime);
}

void ToProto(NProto::TUserStatistics* protoStatistics, const TUserStatistics& statistics)
{
    protoStatistics->set_request_counter(statistics.RequestCounter);
    protoStatistics->set_read_request_timer(statistics.ReadRequestTimer.MicroSeconds());
    protoStatistics->set_write_request_timer(statistics.WriteRequestTimer.MicroSeconds());
    protoStatistics->set_access_time(statistics.AccessTime.MicroSeconds());
}

void FromProto(TUserStatistics* statistics, const NProto::TUserStatistics& protoStatistics)
{
    statistics->RequestCounter = protoStatistics.request_counter();
    statistics->ReadRequestTimer = TDuration(protoStatistics.read_request_timer());
    statistics->WriteRequestTimer = TDuration(protoStatistics.write_request_timer());
    statistics->AccessTime = TInstant(protoStatistics.access_time());
}

void Serialize(const TUserStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("request_counter").Value(statistics.RequestCounter)
            .Item("read_request_timer").Value(statistics.ReadRequestTimer)
            .Item("write_request_timer").Value(statistics.WriteRequestTimer)
            .Item("access_time").Value(statistics.AccessTime)
        .EndMap();
}

TUserStatistics& operator += (TUserStatistics& lhs, const TUserStatistics& rhs)
{
    lhs.RequestCounter += rhs.RequestCounter;
    lhs.ReadRequestTimer += rhs.ReadRequestTimer;
    lhs.WriteRequestTimer += rhs.WriteRequestTimer;
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
    , RequestRateLimit_(100.0)
    , LocalStatisticsPtr_(nullptr)
    , RequestStatisticsUpdateIndex_(-1)
{
    ResetRequestRate();
}

void TUser::Save(NCellMaster::TSaveContext& context) const
{
    TSubject::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, RequestRateLimit_);
    Save(context, MulticellStatistics_);
    Save(context, ClusterStatistics_);
}

void TUser::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    Load(context, RequestRateLimit_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 200) {
        Load(context, MulticellStatistics_);
    }
    Load(context, ClusterStatistics_);
}

void TUser::ResetRequestRate()
{
    CheckpointTime_ = TInstant::Zero();
    CheckpointRequestCounter_ = 0;
    RequestRate_ = 0;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

