#include <yt/yt/client/query_tracker_client/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

using NQueryTrackerClient::TQueryId;
using NQueryTrackerClient::EQueryState;
using NQueryTrackerClient::EQueryEngine;

using TTrackerId = TString;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAlertManager)
DECLARE_REFCOUNTED_CLASS(TAlertManagerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TQueryTracker)
DECLARE_REFCOUNTED_CLASS(TQueryTrackerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TQueryTrackerServerConfig)
DECLARE_REFCOUNTED_CLASS(TQueryTrackerServerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IQueryHandler)
DECLARE_REFCOUNTED_STRUCT(IQueryEngine)
DECLARE_REFCOUNTED_STRUCT(IQueryTracker)

DECLARE_REFCOUNTED_CLASS(TYqlEngineConfig)
DECLARE_REFCOUNTED_CLASS(TChytEngineConfig)
DECLARE_REFCOUNTED_CLASS(TSpytEngineConfig)
DECLARE_REFCOUNTED_CLASS(TQLEngineConfig)
DECLARE_REFCOUNTED_CLASS(TEngineConfigBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker