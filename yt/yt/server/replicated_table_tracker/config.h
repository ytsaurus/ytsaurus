#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
public:
    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    TDuration RttServiceRequestTimeout;

    REGISTER_YSON_STRUCT(TReplicatedTableTrackerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTrackerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerProgramConfig
    : public TReplicatedTableTrackerBootstrapConfig
    , public TServerProgramConfig
{
public:
    REGISTER_YSON_STRUCT(TReplicatedTableTrackerProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTrackerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
