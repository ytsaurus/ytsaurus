#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/queue_client/common.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueAgentStageChannelConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentDynamicStateConfig)
DECLARE_REFCOUNTED_CLASS(TQueueConsumerRegistrationManagerConfig)

inline const TString ProductionStage = "production";

////////////////////////////////////////////////////////////////////////////////

using TRowRevision = ui64;
constexpr TRowRevision NullRowRevision = 0;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReplicaInfo)
DECLARE_REFCOUNTED_CLASS(TChaosReplicaInfo)
DECLARE_REFCOUNTED_CLASS(TReplicatedTableMeta)
DECLARE_REFCOUNTED_CLASS(TChaosReplicatedTableMeta)
DECLARE_REFCOUNTED_CLASS(TGenericReplicatedTableMeta)


DECLARE_REFCOUNTED_CLASS(TQueueTable)
DECLARE_REFCOUNTED_CLASS(TConsumerTable)
DECLARE_REFCOUNTED_CLASS(TConsumerRegistrationTable)
DECLARE_REFCOUNTED_CLASS(TQueueAgentObjectMappingTable)
DECLARE_REFCOUNTED_CLASS(TReplicatedTableMappingTable)
DECLARE_REFCOUNTED_STRUCT(TDynamicState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueConsumerRegistrationManager)

////////////////////////////////////////////////////////////////////////////////

struct TQueueTableRow;
struct TConsumerTableRow;
struct TConsumerRegistrationTableRow;
struct TReplicatedTableMappingTableRow;
using TConsumerRowMap = THashMap<TCrossClusterReference, TConsumerTableRow>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
