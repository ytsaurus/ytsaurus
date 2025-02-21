#pragma once

#include "private.h"

#include <yt/yt/client/kafka/requests.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

struct IGroupCoordinator
    : public TRefCounted
{
    virtual NKafka::TRspJoinGroup JoinGroup(const NKafka::TReqJoinGroup& request, const NLogging::TLogger& Logger) = 0;
    virtual NKafka::TRspSyncGroup SyncGroup(const NKafka::TReqSyncGroup& request, const NLogging::TLogger& Logger) = 0;
    virtual NKafka::TRspHeartbeat Heartbeat(const NKafka::TReqHeartbeat& request, const NLogging::TLogger& Logger) = 0;

    virtual void Reconfigure(const TGroupCoordinatorConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGroupCoordinator)

////////////////////////////////////////////////////////////////////////////////

IGroupCoordinatorPtr CreateGroupCoordinator(TString groupId, TGroupCoordinatorConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namspace NYT::NKafkaProxy
