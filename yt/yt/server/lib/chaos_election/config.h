#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChaosElection {

////////////////////////////////////////////////////////////////////////////////

struct TChaosElectionManagerConfig
    : public NYTree::TYsonStruct
{
    NYPath::TYPath LockTablePath;
    std::string ChaosCellBundle;

    TDuration LeaseTimeout;
    TDuration LeasePingPeriod;
    TDuration LockAcquisitionPeriod;
    TDuration LeaderCacheUpdatePeriod;

    REGISTER_YSON_STRUCT(TChaosElectionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosElection
