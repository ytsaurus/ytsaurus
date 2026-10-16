#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/library/lock_election/config.h>

namespace NYT::NChaosElection {

////////////////////////////////////////////////////////////////////////////////

struct TChaosElectionManagerConfig
    : public NLockElection::TLockElectionManagerConfig
{
    NYPath::TYPath LockTablePath;
    std::string ChaosCellBundle;

    TDuration LeaseTimeout;
    TDuration LeasePingPeriod;

    REGISTER_YSON_STRUCT(TChaosElectionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosElection
