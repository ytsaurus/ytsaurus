#pragma once

#include "private.h"

#include <yt/yt/client/kafka/requests.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

struct IGroupCoordinator
    : public TRefCounted
{
    virtual NKafka::TRspJoinGroup JoinGroup(const NKafka::TReqJoinGroup& request, const NLogging::TLogger& logger) = 0;
    virtual NKafka::TRspSyncGroup SyncGroup(const NKafka::TReqSyncGroup& request, const NLogging::TLogger& logger) = 0;
    virtual NKafka::TRspHeartbeat Heartbeat(const NKafka::TReqHeartbeat& request, const NLogging::TLogger& logger) = 0;
    virtual NKafka::TRspLeaveGroup LeaveGroup(const NKafka::TReqLeaveGroup& request, const NLogging::TLogger& logger) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGroupCoordinator)

////////////////////////////////////////////////////////////////////////////////

struct IGroupCoordinatorManager
    : public TRefCounted
{
    virtual std::optional<IGroupCoordinatorPtr> GetGroupCoordinator(const NKafka::TGroupId& groupId) = 0;
    virtual IGroupCoordinatorPtr GetOrCreateGroupCoordinator(const NKafka::TGroupId& groupId) = 0;
    virtual void OnDynamicConfigChanged(const TGroupCoordinatorConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGroupCoordinatorManager)

////////////////////////////////////////////////////////////////////////////////

IGroupCoordinatorManagerPtr CreateGroupCoordinatorManager();

////////////////////////////////////////////////////////////////////////////////

} // namspace NYT::NKafkaProxy
