#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/queue_client/common.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueAgentStageChannelConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TQueueConsumerRegistrationManagerConfig)

inline const TString ProductionStage = "production";

////////////////////////////////////////////////////////////////////////////////

using TRowRevision = ui64;
constexpr TRowRevision NullRowRevision = 0;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueTable)
DECLARE_REFCOUNTED_CLASS(TConsumerTable)
DECLARE_REFCOUNTED_CLASS(TConsumerRegistrationTable)
DECLARE_REFCOUNTED_STRUCT(TDynamicState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueConsumerRegistrationManager)

////////////////////////////////////////////////////////////////////////////////

struct TQueueTableRow;
struct TConsumerTableRow;
struct TConsumerRegistrationTableRow;
using TConsumerRowMap = THashMap<TCrossClusterReference, TConsumerTableRow>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
