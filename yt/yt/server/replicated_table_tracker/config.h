#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerServerConfig
    : public TNativeServerConfig
    , public TServerProgramConfig
{
public:
    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    TDuration RttServiceRequestTimeout;

    REGISTER_YSON_STRUCT(TReplicatedTableTrackerServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTrackerServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
