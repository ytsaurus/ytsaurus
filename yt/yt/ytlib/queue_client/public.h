#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/queue_client/common.h>

#include <yt/yt/client/hydra/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TQueueAgentStageChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TQueueAgentConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TQueueAgentDynamicStateConfig)
DECLARE_REFCOUNTED_STRUCT(TQueueConsumerRegistrationManagerConfig)

inline const TString ProductionStage = "production";

////////////////////////////////////////////////////////////////////////////////

using TRowRevision = NHydra::TRevision;
constexpr auto NullRowRevision = NHydra::NullRevision;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TReplicaInfo)
DECLARE_REFCOUNTED_STRUCT(TChaosReplicaInfo)
DECLARE_REFCOUNTED_STRUCT(TReplicatedTableMeta)
DECLARE_REFCOUNTED_STRUCT(TChaosReplicatedTableMeta)
DECLARE_REFCOUNTED_STRUCT(TGenericReplicatedTableMeta)


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
