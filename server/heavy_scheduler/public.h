#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

#include <yp/server/lib/cluster/public.h>

#include <yp/server/lib/objects/public.h>

#include <yp/client/api/native/public.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;
DECLARE_REFCOUNTED_CLASS(TYTConnector);
DECLARE_REFCOUNTED_CLASS(THeavyScheduler);
DECLARE_REFCOUNTED_CLASS(TAntiaffinityHealer);
DECLARE_REFCOUNTED_CLASS(TSwapDefragmentator);
DECLARE_REFCOUNTED_CLASS(TDisruptionThrottler);
DECLARE_REFCOUNTED_STRUCT(ITask);

DECLARE_REFCOUNTED_CLASS(TYTConnectorConfig);
DECLARE_REFCOUNTED_CLASS(TClusterReaderConfig);
DECLARE_REFCOUNTED_CLASS(TTaskManagerConfig);
DECLARE_REFCOUNTED_CLASS(TDisruptionThrottlerConfig);
DECLARE_REFCOUNTED_CLASS(TSwapDefragmentatorConfig);
DECLARE_REFCOUNTED_CLASS(TAntiaffinityHealerConfig);
DECLARE_REFCOUNTED_CLASS(THeavySchedulerConfig);
DECLARE_REFCOUNTED_CLASS(THeavySchedulerProgramConfig);

////////////////////////////////////////////////////////////////////////////////

class TResourceVector;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETaskSource,
    (SwapDefragmentator)
    (AntiaffinityHealer)
);

////////////////////////////////////////////////////////////////////////////////

using NObjects::EObjectType;

using NObjects::TObjectId;

using NObjects::NullTimestamp;
using NObjects::TTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
