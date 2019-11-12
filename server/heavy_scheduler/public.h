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

DECLARE_REFCOUNTED_CLASS(TYTConnectorConfig);
DECLARE_REFCOUNTED_CLASS(TClusterReaderConfig);
DECLARE_REFCOUNTED_CLASS(THeavySchedulerConfig);
DECLARE_REFCOUNTED_CLASS(THeavySchedulerProgramConfig);

////////////////////////////////////////////////////////////////////////////////

class TResourceVector;

////////////////////////////////////////////////////////////////////////////////

using NObjects::EObjectType;

using NObjects::TObjectId;

using NObjects::NullTimestamp;
using NObjects::TTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
