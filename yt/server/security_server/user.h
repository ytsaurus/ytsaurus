#pragma once

#include "public.h"
#include "subject.h"

#include <core/misc/property.h>

#include <core/yson/consumer.h>

#include <server/object_server/object.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TUserStatistics
{
    i64 RequestCounter = 0;
    TDuration ReadRequestTimer;
    TDuration WriteRequestTimer;
    TInstant AccessTime;

    void Persist(NCellMaster::TPersistenceContext& context);
};

void ToProto(NProto::TUserStatistics* protoStatistics, const TUserStatistics& statistics);
void FromProto(TUserStatistics* statistics, const NProto::TUserStatistics& protoStatistics);

void Serialize(const TUserStatistics& statistics, NYson::IYsonConsumer* consumer);

TUserStatistics& operator += (TUserStatistics& lhs, const TUserStatistics& rhs);
TUserStatistics  operator +  (const TUserStatistics& lhs, const TUserStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

class TUser
    : public TSubject
{
public:
    // Limits and bans.
    DEFINE_BYVAL_RW_PROPERTY(bool, Banned);
    DEFINE_BYVAL_RW_PROPERTY(double, RequestRateLimit);

    // Statistics
    using TMulticellStatistics = yhash_map<NObjectClient::TCellTag, TUserStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TMulticellStatistics, MulticellStatistics);
    DEFINE_BYVAL_RW_PROPERTY(TUserStatistics*, LocalStatisticsPtr);
    DEFINE_BYREF_RW_PROPERTY(TUserStatistics, ClusterStatistics);
    DEFINE_BYVAL_RW_PROPERTY(int, RequestStatisticsUpdateIndex);
    
    // Request rate management.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, CheckpointTime);
    DEFINE_BYVAL_RW_PROPERTY(i64, CheckpointRequestCounter);
    DEFINE_BYVAL_RW_PROPERTY(double, RequestRate);

public:
    explicit TUser(const TUserId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void ResetRequestRate();

    TUserStatistics& CellStatistics(NObjectClient::TCellTag cellTag);
    TUserStatistics& LocalStatistics();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
