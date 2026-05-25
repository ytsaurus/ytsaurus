#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCypressElection {

////////////////////////////////////////////////////////////////////////////////

struct TCypressElectionManagerConfig
    : public NYTree::TYsonStruct
{
    NYPath::TYPath LockPath;

    TDuration TransactionTimeout;
    TDuration TransactionPingPeriod;
    TDuration LockAcquisitionPeriod;
    TDuration LeaderCacheUpdatePeriod;

    REGISTER_YSON_STRUCT(TCypressElectionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
